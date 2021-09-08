package com.chris.configuration;

import com.chris.util.ConnectUtil;
import common.DBTypeEnum;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class Configuration {

    private String configId;
    private final WriterConfiguration writerConfiguration;
    private final ReaderConfiguration readerConfiguration;
    private final SyncDataConfiguration syncDataConfiguration;
    public DBTypeEnum dbType;
    private String hostname;
    private String dbName;
    private String port;
    private String user;
    private String password;
    private String tableName;
    private Connection connection;
    private final Logger logger = Logger.getLogger(Configuration.class);

    public Configuration() {
        writerConfiguration = new WriterConfiguration();
        readerConfiguration = new ReaderConfiguration();
        syncDataConfiguration = new SyncDataConfiguration();
    }

    public void config (String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            logger.error(e);
        }

        dbType = common.DBTypeEnum.valueOf(properties.getProperty("config.dbType").toUpperCase());
        hostname = properties.getProperty("config.hostname");
        dbName = properties.getProperty("config.dbName");
        port = properties.getProperty("config.port");
        user = properties.getProperty("config.user");
        password = properties.getProperty("config.password");
        tableName = properties.getProperty("config.tableName");
    }

    public String getUrl() {
        return "jdbc:" + dbType.toString().toLowerCase() + "://" + hostname + ":" + port + "/" + dbName;
    }

    private void connect() {
        connection = ConnectUtil.connect(dbType, getUrl(), user, password);
    }

    public void configInfo() {
        try {
            connect();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from " + tableName + " where config_id=" + configId);

            resultSet.next(); // 光标默认位于第一行之前，需要移动至下一行
            writerConfiguration.setDBType(DBTypeEnum.valueOf(resultSet.getString("writer_db_type")));
            writerConfiguration.setHostname(resultSet.getString("writer_hostname"));
            writerConfiguration.setPort(resultSet.getString("writer_port"));
            writerConfiguration.setDBName(resultSet.getString("writer_db_name"));
            writerConfiguration.setUser(resultSet.getString("writer_user"));
            writerConfiguration.setPassword(resultSet.getString("writer_password"));
            writerConfiguration.setTableName(resultSet.getString("writer_table_name"));
            readerConfiguration.setDBType(DBTypeEnum.valueOf(resultSet.getString("reader_db_type")));
            readerConfiguration.setHostname(resultSet.getString("reader_hostname"));
            readerConfiguration.setPort(resultSet.getString("reader_port"));
            readerConfiguration.setDBName(resultSet.getString("reader_db_name"));
            readerConfiguration.setUser(resultSet.getString("reader_user"));
            readerConfiguration.setPassword(resultSet.getString("reader_password"));
            readerConfiguration.setTableName(resultSet.getString("reader_table_name"));
            syncDataConfiguration.setInterval(Integer.parseInt(resultSet.getString("sync_interval")));
        } catch (SQLException e) {
            logger.error(e);
        }

        close();
    }

    private void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public void setConfigId(String configId) {
        this.configId = configId;
    }

    public WriterConfiguration getWriterConfig() {
        return writerConfiguration;
    }

    public ReaderConfiguration getReaderConfig() {
        return readerConfiguration;
    }

    public SyncDataConfiguration getSyncDataConfig() {
        return syncDataConfiguration;
    }
}
