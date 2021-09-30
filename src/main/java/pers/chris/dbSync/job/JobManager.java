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
import pers.chris.dbSync.valueFilter.ValueFilter;
import pers.chris.dbSync.valueFilter.ValueFilterManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class JobManager {

    List<Job> jobs;
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
        jobs = new ArrayList<>();
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

    public void connect() {
        String url = ConnectUtil.getUrl(dbType, hostname, port, dbName);
        connection = ConnectUtil.connect(dbType, url, user, password);
    }

    public void load() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + jobConfTableName);

            while (resultSet.next()) {
                Job job = new Job();
                JobConf jobConf = new JobConf();

                job.setJobId(resultSet.getString("job_id"));
                jobConf.jobType = JobTypeEnum.valueOf(resultSet.getString("job_type"));
                jobConf.setSrcDBConfId(resultSet.getString("src_db_conf_id"));
                jobConf.setDstDBConfId(resultSet.getString("dst_db_conf_id"));
                jobConf.setSyncConfId(resultSet.getString("sync_conf_id"));

                job.setJobConf(jobConf);
                jobs.add(job);
            }
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    private JobConf readJobConf(String jobId) {
        JobConf jobConf = new JobConf();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + jobConfTableName
            + " where job_id=" + jobId);

            resultSet.next();

            jobConf.jobType = JobTypeEnum.valueOf(resultSet.getString("job_type"));
            jobConf.setSrcDBConfId(resultSet.getString("src_db_conf_id"));
            jobConf.setDstDBConfId(resultSet.getString("dst_db_conf_id"));
            jobConf.setSyncConfId(resultSet.getString("sync_conf_id"));
        } catch (SQLException e) {
            logger.error(e);
        }
        return jobConf;
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
            dbConf.setHostname(resultSet.getString("hostname"));
            dbConf.setPort(resultSet.getString("port"));
            dbConf.setDBName(resultSet.getString("db_name"));
            dbConf.setUser(resultSet.getString("user"));
            dbConf.setPassword(resultSet.getString("password"));
            dbConf.setTableName(resultSet.getString("table_name"));
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
            syncConf.setInterval(Integer.parseInt(resultSet.getString("interval")));
            syncConf.setTimeField(resultSet.getString("time_field_name"));
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
        String jobId = job.getJobId();
        JobConf jobConf = job.getJobConf();

        job.setSrcDBConf(readDBConf(jobConf.getSrcDBConfId()));
        job.setDstDBConf(readDBConf(jobConf.getDstDBConfId()));
        job.setSyncConf(readSyncConf(jobConf.getSyncConfId()));
        job.setValueFilterManager(new ValueFilterManager(readValueFilterConf(jobId)));
        job.setFieldMapManager(new FieldMapManager(readFieldMapConf(jobId)));
    }

    public void run() {
        for (Job job : jobs) {
            jobInit(job);

            Thread jobThead = new Thread(job);
            jobThead.start();
        }
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e);
        }
    }

}
