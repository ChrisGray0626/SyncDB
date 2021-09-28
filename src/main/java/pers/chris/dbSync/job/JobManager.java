package pers.chris.dbSync.job;

import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.fieldMap.FieldMapManager;
import pers.chris.dbSync.util.ConnectUtil;
import pers.chris.dbSync.common.typeEnum.DBTypeEnum;
import org.apache.log4j.Logger;
import pers.chris.dbSync.util.ResultSetParseUtil;
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

    public void add(String jobId) {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + syncJobConfTableName
                            + " Where job_id=" + jobId);
            jobs.add(ResultSetParseUtil.parseJob(resultSet));
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public void load() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + syncJobConfTableName);
            jobs = ResultSetParseUtil.parseJobs(resultSet);
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    private DBConf readDBConf(String confId) {
        DBConf dbConf = null;

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + DBConfTableName
            + " where conf_id=" + confId);
            dbConf = ResultSetParseUtil.parseDBConf(resultSet);
        } catch (SQLException e) {
            logger.error(e);
        }
        return dbConf;
    }

    private List<String> readValueFilterRule(String jobId) {
        List<String> rules = new ArrayList<>();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + valueFilterConfTableName
                            + " where job_id=" + jobId);
            rules = ResultSetParseUtil.parseValueFilterConf(resultSet);
        } catch (SQLException e) {
            logger.error(e);
        }
        return rules;
    }

    private List<String> readFieldMapRule(String jobId) {
        List<String> rules = new ArrayList<>();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + fieldMapConfTableName
            + " where job_id=" + jobId);
            rules = ResultSetParseUtil.parseFieldMapConf(resultSet);
        } catch (SQLException e) {
            logger.debug(e);
        }
        return rules;
    }

    private void jobInit(Job job) {
        String jobId = job.getJobId();
        String srcDBConfId = job.getSrcDBConfId();
        String dstDBConfId = job.getDstDBConfId();

        // 源&目标数据库读取根据DBConfId
        DBConf srcDBConf = readDBConf(srcDBConfId);
        DBConf dstDBConf = readDBConf(dstDBConfId);

        // 数据过滤管理器&字段映射管理器读取&加载根据JobId
        List<String> valueFilterRules = readValueFilterRule(jobId);
        List<String> fieldMapRules = readFieldMapRule(jobId);
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
