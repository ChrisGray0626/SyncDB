package pers.chris.dbSync.job;

import pers.chris.dbSync.common.typeEnum.JobTypeEnum;
import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.conf.JobConf;
import pers.chris.dbSync.fieldMap.FieldMapManager;
import pers.chris.dbSync.util.ConnectUtil;
import pers.chris.dbSync.common.typeEnum.DBTypeEnum;
import org.apache.log4j.Logger;
import pers.chris.dbSync.valueFilter.ValueFilterManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JobManager {

    List<Job> jobs;
    public DBTypeEnum dbType;
    private String hostname;
    private String dbName;
    private String port;
    private String user;
    private String password;
    private String syncJobConfTableName;
    private String DBConfTableName;
    private String fieldMapConfTableName;
    private String valueFilterConfTableName;
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
        syncJobConfTableName = properties.getProperty("conf.syncJobConfTableName");
        DBConfTableName = properties.getProperty("conf.DBConfTableName");
        fieldMapConfTableName = properties.getProperty("conf.fieldMapConfTableName");
        valueFilterConfTableName = properties.getProperty("conf.valueFilterConfTableName");
    }

    public void connect() {
        String url = ConnectUtil.getUrl(dbType, hostname, port, dbName);
        connection = ConnectUtil.connect(dbType, url, user, password);
    }

    public void load() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + syncJobConfTableName);

            while (resultSet.next()) {
                Job job = new Job();
                JobConf jobConf = new JobConf();

                job.setJobId(resultSet.getString("job_id"));
                jobConf.jobType = JobTypeEnum.valueOf(resultSet.getString("job_type"));
                jobConf.setSrcDBConfId(resultSet.getString("src_db_conf_id"));
                jobConf.setDstDBConfId(resultSet.getString("dst_db_conf_id"));
                jobConf.setInterval(Integer.parseInt(resultSet.getString("sync_interval")));
                jobConf.setTimeField(resultSet.getString("sync_time_field_name"));
                job.setJobConf(jobConf);

                jobs.add(job);
            }
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    private DBConf readDBConf(String confId) {
        DBConf dbConf = new DBConf();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + DBConfTableName
            + " where conf_id=" + confId);

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

    private List<String> readValueFilterConf(String jobId) {
        List<String> rules = new ArrayList<>();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + valueFilterConfTableName
                            + " where job_id=" + jobId);

            while (resultSet.next()) {
                String rule = resultSet.getString("rule");
                rules.add(rule);
            }
        } catch (SQLException e) {
            logger.error(e);
        }
        return rules;
    }

    private List<String> readFieldMapConf(String jobId) {
        List<String> rules = new ArrayList<>();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + fieldMapConfTableName
            + " where job_id=" + jobId);

            while (resultSet.next()) {
                String rule = resultSet.getString("rule");
                rules.add(rule);
            }
        } catch (SQLException e) {
            logger.debug(e);
        }
        return rules;
    }

    private void jobInit(Job job) {
        String jobId = job.getJobId();
        JobConf jobConf = job.getJobConf();

        // 源&目标数据库读取根据DBConfId
        String srcDBConfId = jobConf.getSrcDBConfId();
        String dstDBConfId = jobConf.getDstDBConfId();
        DBConf srcDBConf = readDBConf(srcDBConfId);
        DBConf dstDBConf = readDBConf(dstDBConfId);

        // 数据过滤管理器&字段映射管理器读取&加载根据JobId
        List<String> valueFilterRules = readValueFilterConf(jobId);
        List<String> fieldMapRules = readFieldMapConf(jobId);
        ValueFilterManager valueFilterManager = new ValueFilterManager(valueFilterRules);
        FieldMapManager fieldMapManager = new FieldMapManager(fieldMapRules);

        job.setSrcDBConf(srcDBConf);
        job.setDstDBConf(dstDBConf);
        job.setValueFilterManager(valueFilterManager);
        job.setFieldMapManager(fieldMapManager);
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
