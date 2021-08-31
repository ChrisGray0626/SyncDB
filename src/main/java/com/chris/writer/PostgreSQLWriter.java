package com.chris.writer;

import com.chris.syncData.SyncData;
import com.chris.syncData.SyncDataEvent;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PostgreSQLWriter extends AbstractWriter {
    public WriterTypeEnum writerType;
    private SyncData syncData;
    private String url;
    private String username;
    private String password;
    private String tableName;
    private Connection connection;
    private Statement statement;
    private static final Logger logger = Logger.getLogger(PostgreSQLWriter.class);

    public PostgreSQLWriter() {
        writerType = WriterTypeEnum.POSTGRESQL;
    }

    @Override
    public void config(String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        username = properties.getProperty("writer.username");
        String databaseName = properties.getProperty("writer.databaseName");
        password = properties.getProperty("writer.password");
        tableName = properties.getProperty("writer.tableName");
        String hostname = properties.getProperty("writer.hostname");
        String port = properties.getProperty("writer.port");
        url = "jdbc:postgresql://" + hostname + ":" + port + "/" + databaseName;

    }

    @Override
    public void init(SyncData syncData) {
        syncData.setWriterType(writerType);
        this.syncData = syncData;
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
    public void write() {
        syncData.registerListener(new SyncData.SyncDataListener() {
            @Override
            public void doSet(SyncDataEvent event) {
                logger.debug(syncData.toString());
                List<List<String>> rowsData = syncData.getRowsData();
                SyncData.EventTypeEnum curEventTypeEnum = syncData.getEventType();
                List<String> SQLs = new ArrayList<>();

                if (curEventTypeEnum == SyncData.EventTypeEnum.INSERT) {
                    SQLs = insertSQL(rowsData);
                }

                for (String SQL: SQLs) {
                    try {
                        System.out.println(SQL);
                        statement.execute(SQL);
                    } catch (SQLException e) {
                        logger.error(e);
                    }
                }
            }
        });
    }

    private List<String> insertSQL(List<List<String>> rowsData) {
        List<String> SQLs = new ArrayList<>();

        for (List<String> rows : rowsData) {
            StringBuilder values = new StringBuilder();
            for (String value : rows) {
                values.append("'").append(value).append("',");
            }
            values.deleteCharAt(values.length() - 1);

            String SQL = "INSERT INTO " + tableName + " VALUES(" + values + ")";
            SQLs.add(SQL);
        }
        return SQLs;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e);
        }
    }
}
