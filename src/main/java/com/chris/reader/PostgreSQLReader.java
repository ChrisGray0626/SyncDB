package com.chris.reader;

import com.chris.syncData.SyncData;
import com.chris.util.ParseUtil;
import com.chris.writer.PostgreSQLWriter;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PostgreSQLReader extends AbstractReader{
    public ReaderTypeEnum readerType = ReaderTypeEnum.POSTGRESQL;
    private SyncData syncData;
    private String url;
    private String username;
    private String password;
    private Connection connection;
    private Statement statement;
    private String logicalReplicationSlotName = "test_slot";
    private static final Logger logger = Logger.getLogger(PostgreSQLWriter.class);

    public PostgreSQLReader() {
    }

    @Override
    public void init(SyncData syncData) {
        this.syncData = syncData;
        syncData.setReaderType(readerType);
    }

    @Override
    public void config(String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            logger.error(e);
        }
        String hostname = properties.getProperty("reader.hostname");
        String port = properties.getProperty("reader.port");
        String databaseName = properties.getProperty("reader.databaseName");
        url = "jdbc:postgresql://" + hostname + ":" + port + "/" + databaseName;
        username = properties.getProperty("reader.username");
        password = properties.getProperty("reader.password");
    }

    @Override
    public void connect() {
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            logger.error(e);
        }
    }

    @Override
    public void read() {
        while (true) {
            readLogicalSlot(syncData);
            try {
                TimeUnit.MINUTES.sleep(5);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    public void read(Integer minutes) {
        while (true) {
            readLogicalSlot(syncData);
            try {
                TimeUnit.MINUTES.sleep(minutes);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    private void readLogicalSlot(SyncData syncData) {
        try {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_logical_slot_get_changes('" + logicalReplicationSlotName + "', NULL, NULL)");
            ParseUtil.parsePGLogicalSlot(resultSet, syncData);
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
