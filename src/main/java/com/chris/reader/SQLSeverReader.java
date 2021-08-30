package com.chris.reader;

import com.chris.syncData.SyncData;
import com.chris.util.ParseUtil;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SQLSeverReader extends AbstractReader{

    public ReaderTypeEnum readerType = ReaderTypeEnum.SQLSERVER;
    private SyncData syncData;
    private String url;
    private String username;
    private String password;
    private Connection connection;
    private Statement statement;
    private static final Logger logger = Logger.getLogger(SQLSeverReader.class);

    public SQLSeverReader() {
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
        url = "jdbc:sqlserver://" + hostname + ":" + port + ";DatabaseName=" + databaseName;
        username = properties.getProperty("reader.username");
        password = properties.getProperty("reader.password");
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
        while (true) {
            readCDCTable(syncData);
            try {
                TimeUnit.MINUTES.sleep(5);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    public void read(Integer minutes) {
        while (true) {
            readCDCTable(syncData);
            try {
                TimeUnit.MINUTES.sleep(minutes);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    private void readCDCTable(SyncData syncData) {
        try {
            statement.execute("DECLARE @bglsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('smallest greater than or equal',DATEADD(mi,-5,GETDATE()))");
            statement.execute("DECLARE @edlsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('largest less than or equal',GETDATE())");
            ResultSet resultSet = statement.executeQuery(
                    "DECLARE @bglsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('smallest greater than or equal',DATEADD(mi,-5,GETDATE())); "
                    + "DECLARE @edlsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('largest less than or equal',GETDATE())"
                    + "SELECT * FROM cdc.dbo_" + syncData.getTableName() + "_CT WHERE [__$start_lsn] BETWEEN @bglsn AND @edlsn");
            ParseUtil.parseSQLServerCDC(resultSet, syncData);
        } catch (SQLException | NullPointerException e) {
            logger.error(e);
        }
    }

    // TODO 重载轮询方法 查询时间

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
