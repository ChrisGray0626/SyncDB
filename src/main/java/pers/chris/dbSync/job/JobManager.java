package pers.chris.dbSync.job;

import pers.chris.dbSync.util.ConnectUtil;
import pers.chris.dbSync.common.DBTypeEnum;
import org.apache.log4j.Logger;
import pers.chris.dbSync.util.ResultSetParseUtil;

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
    private String tableName;
    private Connection connection;
    private final Logger logger = Logger.getLogger(JobManager.class);

    public JobManager() {
        jobs = new ArrayList<>();
    }

    public void config (String fileName) {
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
        tableName = properties.getProperty("conf.tableName");
    }

    private void connect() {
        String url = ConnectUtil.getUrl(dbType, hostname, port, dbName);
        connection = ConnectUtil.connect(dbType, url, user, password);
    }

    public void readJobConf() {
        connect();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + tableName);
            jobs = ResultSetParseUtil.parseJobConf(resultSet);
        } catch (SQLException e) {
            logger.error(e);
        }

        close();
    }

    public void run() {
        for (Job job : jobs) {
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
