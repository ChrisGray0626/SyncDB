package pers.chris.dbSync.job;

import pers.chris.dbSync.common.typeEnum.JobTypeEnum;
import pers.chris.dbSync.common.typeEnum.SyncTypeEnum;
import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.conf.JobConf;
import pers.chris.dbSync.conf.SyncConf;
import pers.chris.dbSync.fieldMap.FieldMapManager;
import pers.chris.dbSync.fieldMap.FieldMapper;
import pers.chris.dbSync.util.ConnectUtil;
import pers.chris.dbSync.common.typeEnum.DBTypeEnum;
import org.apache.log4j.Logger;
import pers.chris.dbSync.util.TimeUtil;
import pers.chris.dbSync.valueFilter.ValueFilter;
import pers.chris.dbSync.valueFilter.ValueFilterManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JobManager {

    private final ExecutorService executorService;
    private final Map<String, Job> jobs;
    public DBTypeEnum dbType;
    private String hostname;
    private String dbName;
    private String port;
    private String user;
    private String password;
    private String jobConfTableName;
    private String DBConfTableName;
    private String fieldMapConfTableName;
    private String valueFilterConfTableName;
    private String syncConfTableName;
    private Connection connection;
    private final Logger logger = Logger.getLogger(JobManager.class);

    public JobManager() {
        executorService = Executors.newCachedThreadPool();
        jobs = new HashMap<>();
    }

    public void init(String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            logger.error(e);
        }

        dbType = DBTypeEnum.valueOf(properties.getProperty("conf.dbType").toUpperCase());
        hostname = properties.getProperty("conf.hostname");
        dbName = properties.getProperty("conf.dbName");
        port = properties.getProperty("conf.port");
        user = properties.getProperty("conf.user");
        password = properties.getProperty("conf.password");
        jobConfTableName = properties.getProperty("conf.jobConfTableName");
        DBConfTableName = properties.getProperty("conf.DBConfTableName");
        fieldMapConfTableName = properties.getProperty("conf.fieldMapConfTableName");
        valueFilterConfTableName = properties.getProperty("conf.valueFilterConfTableName");
        syncConfTableName = properties.getProperty("conf.syncConfTableName");
    }

    private void connect() {
        String url = ConnectUtil.getUrl(dbType, hostname, port, dbName);
        connection = ConnectUtil.connect(dbType, url, user, password);
    }

    public void load() {
        connect();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + jobConfTableName);

            while (resultSet.next()) {
                Job job = new Job();
                JobConf jobConf = new JobConf();

                job.setJobId(resultSet.getString("job_id"));
                jobConf.jobType = JobTypeEnum.valueOf(resultSet.getString("job_type"));
                jobConf.srcDBConfId = resultSet.getString("src_db_conf_id");
                jobConf.dstDBConfId = resultSet.getString("dst_db_conf_id");
                jobConf.syncConfId = resultSet.getString("sync_conf_id");

                job.setJobConf(jobConf);
                jobs.put(job.getJobId(), job);
            }
        } catch (SQLException e) {
            logger.error(e);
        }

        close();
    }

    private DBConf readDBConf(String DBConfId) {
        DBConf dbConf = new DBConf();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + DBConfTableName
            + " where db_id=" + DBConfId);

            resultSet.next();
            dbConf.dbType = DBTypeEnum.valueOf(resultSet.getString("db_type"));
            dbConf.hostname = resultSet.getString("hostname");
            dbConf.port = resultSet.getString("port");
            dbConf.dbName = resultSet.getString("db_name");
            dbConf.user = resultSet.getString("user");
            dbConf.password = resultSet.getString("password");
            dbConf.tableName = resultSet.getString("table_name");
        } catch (SQLException e) {
            logger.error(e);
        }
        return dbConf;
    }

    private SyncConf readSyncConf(String syncConfId) {
        SyncConf syncConf = new SyncConf();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + syncConfTableName
                            + " where sync_id=" + syncConfId);

            resultSet.next();

            syncConf.syncType = SyncTypeEnum.valueOf(resultSet.getString("sync_type"));
            syncConf.interval = Integer.parseInt(resultSet.getString("interval"));
            syncConf.timeField = resultSet.getString("time_field_name");
        } catch (SQLException e) {
            logger.debug(e);
        }
        return syncConf;
    }

    private Map<String, ValueFilter> readValueFilterConf(String jobId) {
        Map<String, ValueFilter> valueFilters = new HashMap<>();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + valueFilterConfTableName
                            + " where job_id=" + jobId);

            while (resultSet.next()) {
                String filterId = resultSet.getString("filter_id");
                String rule = resultSet.getString("rule");

                valueFilters.put(filterId, new ValueFilter(rule));
            }
        } catch (SQLException e) {
            logger.error(e);
        }
        return valueFilters;
    }

    private Map<String, FieldMapper> readFieldMapConf(String jobId) {
        Map<String, FieldMapper> fieldMappers = new HashMap<>();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + fieldMapConfTableName
            + " where job_id=" + jobId);

            while (resultSet.next()) {
                String mapId = resultSet.getString("map_id");
                String rule = resultSet.getString("rule");

                fieldMappers.put(mapId, new FieldMapper(rule));
            }
        } catch (SQLException e) {
            logger.debug(e);
        }
        return fieldMappers;
    }

    private void jobInit(Job job) {
        connect();

        String jobId = job.getJobId();
        JobConf jobConf = job.getJobConf();

        job.setSrcDBConf(readDBConf(jobConf.srcDBConfId));
        job.setDstDBConf(readDBConf(jobConf.dstDBConfId));
        job.setSyncConf(readSyncConf(jobConf.syncConfId));
        job.setValueFilterManager(new ValueFilterManager(readValueFilterConf(jobId)));
        job.setFieldMapManager(new FieldMapManager(readFieldMapConf(jobId)));

        close();
    }

    public void run() {
        for (Job job : jobs.values()) {
            jobInit(job);
            Thread thread = new Thread(job);
            executorService.execute(thread);
        }
    }

    // 线程运行，根据jobId
    public void run(String jobId) {
        Job job = jobs.get(jobId);
        jobInit(job);
        executorService.execute(job);
    }

    // 线程中断，根据jobId
    public void interrupt(String jobId) {
        Thread thread = Job.curThreads.get(jobId);
        thread.interrupt();
    }

    private void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e);
        }
    }

}
