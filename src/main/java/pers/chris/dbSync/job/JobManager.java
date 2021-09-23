package pers.chris.dbSync.job;

import pers.chris.dbSync.fieldMap.FieldMapManager;
import pers.chris.dbSync.util.ConnectUtil;
import pers.chris.dbSync.common.DBTypeEnum;
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

    public void addAll() {
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + syncJobConfTableName);
            jobs = ResultSetParseUtil.parseJobs(resultSet);
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public List<String> readValueFilterRule(String jobId) {
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

    public List<String> readFieldMapRule(String jobId) {
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

    public void run() {
        for (Job job : jobs) {
            String jobId = job.getJobId();

            // 数据过滤管理器&字段映射管理器读取&加载&传入根据JobId
            connect();
            List<String> valueFilterRules = readValueFilterRule(jobId);
            List<String> fieldMapRules = readFieldMapRule(jobId);
            close();

            ValueFilterManager valueFilterManager = new ValueFilterManager(valueFilterRules);
            FieldMapManager fieldMapManager = new FieldMapManager(fieldMapRules);

            job.setValueFilterManager(valueFilterManager);
            job.setFieldMapManager(fieldMapManager);

            Thread jobThead = new Thread(job);
            jobThead.start();
        }
    }

    private void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e);
        }
    }

}
