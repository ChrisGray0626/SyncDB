package com.chris.reader;

import com.chris.syncData.SyncData;
import com.chris.util.ParseUtil;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SQLSeverReader extends AbstractReader {

    public ReaderTypeEnum readerType;
    private SyncData syncData;
    private String url;
    private String username;
    private String password;
    private String tableName;
    private Connection connection;
    private Statement statement;
    private static final Logger logger = Logger.getLogger(SQLSeverReader.class);

    public SQLSeverReader() {
        readerType = ReaderTypeEnum.SQLSERVER;
    }

    @Override
    public void initSyncData(SyncData syncData) {
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
        url = "jdbc:sqlserver://" + hostname + ":" + port + ";DatabaseName=" + databaseName;
        username = properties.getProperty("reader.username");
        password = properties.getProperty("reader.password");
        tableName = properties.getProperty("reader.tableName");
    }

    @Override
    public void connect() {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            connection = DriverManager.getConnection(url, username, password);
            statement = connection.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            logger.error(e);
        }
    }

    @Override
    public void read() {
        // 默认五分钟轮询
        read(5);
    }

    public void read(Integer delayTime) {
        while (true) {
            readCDCTable(syncData, delayTime);
            try {
                TimeUnit.MINUTES.sleep(delayTime);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    // 读取指定表的CDC表
    private void readCDCTable (SyncData syncData, Integer delayTime) {
        try {
            // 查询最近delayTime时间（分钟）内的捕获数据
            ResultSet resultSet = statement.executeQuery(
                    "DECLARE @bglsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('smallest greater than or equal',DATEADD(mi,-" + delayTime + ",GETDATE()));"
                    + "DECLARE @edlsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('largest less than or equal',GETDATE());"
                    + "SELECT * FROM cdc.dbo_" + tableName + "_CT WHERE [__$start_lsn] BETWEEN @bglsn AND @edlsn");
            ParseUtil.parseSQLServerCDC(resultSet, syncData);
        } catch (SQLException | NullPointerException e) {
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
