package com.chris.config;

import com.chris.reader.ReaderTypeEnum;
import com.chris.writer.WriterTypeEnum;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class Config {

    private String configId;
    private final WriterConfig writerConfig;
    private final ReaderConfig readerConfig;
    private final SyncDataConfig syncDataConfig;
    private String url;
    private String user;
    private String password;
    private String tableName;
    private Connection connection;
    private Statement statement;
    private final Logger logger = Logger.getLogger(Config.class);

    public Config () {
        writerConfig = new WriterConfig();
        readerConfig = new ReaderConfig();
        syncDataConfig = new SyncDataConfig();
    }

    public void config (String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            logger.error(e);
        }

        configId = properties.getProperty("config.configId");
        String hostname = properties.getProperty("config.hostname");
        String databaseName = properties.getProperty("config.databaseName");
        String port = properties.getProperty("config.port");
        url = "jdbc:mysql://" + hostname + ":" + port + "/" + databaseName;
        user = properties.getProperty("config.user");
        password = properties.getProperty("config.password");
        tableName = properties.getProperty("config.tableName");
    }

    private void connect() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            logger.error(e);
        }
        try {
            connection = DriverManager.getConnection(url, user, password);
            statement = connection.createStatement();
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public void getConfigInfo() {
        try {
            connect();

            ResultSet resultSet = statement.executeQuery("select * from " + tableName + " where config_id=" + configId);
            resultSet.next(); // 光标默认位于第一行之前，需要移动至下一行
            writerConfig.setWriterType(WriterTypeEnum.valueOf(resultSet.getString("writer_type")));
            writerConfig.setHostname(resultSet.getString("writer_hostname"));
            writerConfig.setPort(resultSet.getString("writer_port"));
            writerConfig.setDatabaseName(resultSet.getString("writer_database_name"));
            writerConfig.setUser(resultSet.getString("writer_user"));
            writerConfig.setPassword(resultSet.getString("writer_password"));
            writerConfig.setTableName(resultSet.getString("writer_table_name"));
            readerConfig.setReaderType(ReaderTypeEnum.valueOf(resultSet.getString("reader_type")));
            readerConfig.setHostname(resultSet.getString("reader_hostname"));
            readerConfig.setPort(resultSet.getString("reader_port"));
            readerConfig.setDatabaseName(resultSet.getString("reader_database_name"));
            readerConfig.setUser(resultSet.getString("reader_user"));
            readerConfig.setPassword(resultSet.getString("reader_password"));
            readerConfig.setTableName(resultSet.getString("reader_table_name"));
            syncDataConfig.setInterval(Integer.parseInt(resultSet.getString("sync_interval")));
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

    public WriterConfig getWriterConfig() {
        return writerConfig;
    }

    public ReaderConfig getReaderConfig() {
        return readerConfig;
    }

    public SyncDataConfig getSyncDataConfig() {
        return syncDataConfig;
    }
}
