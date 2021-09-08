package com.chris.reader;

import com.chris.syncData.SyncData;
import com.chris.util.ConnectUtil;
import com.chris.util.FieldUtil;
import com.chris.util.ParseUtil;
import common.DBTypeEnum;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.concurrent.TimeUnit;

public class SQLServerReader extends AbstractReader {

    private Connection connection;
    private static final Logger logger = Logger.getLogger(SQLServerReader.class);

    public SQLServerReader() {
        dbType = DBTypeEnum.SQLSERVER;
    }

    @Override
    public void connect() {
        connection = ConnectUtil.connect(dbType, getReaderConfig().getUrl(), getReaderConfig().getUser(), getReaderConfig().getPassword());
        setFieldNames(FieldUtil.readFieldName(connection, getReaderConfig().getTableName()));
    }

    @Override
    public void read(Integer interval) {
        while (true) {
            readCDCTable(super.getSyncData(), interval);
            try {
                TimeUnit.MINUTES.sleep(interval);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    @Override
    // 默认轮询间隔1分钟
    public void read() {
        read(1);
    }

    // 读取指定表的CDC表
    private void readCDCTable (SyncData syncData, Integer interval) {
        try {
            // 查询最近 interval 时间（分钟）内的捕获数据
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "DECLARE @bglsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('smallest greater than or equal',DATEADD(mi,-" + interval + ",GETDATE()));"
                    + "DECLARE @edlsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('largest less than or equal',GETDATE());"
                    + "SELECT * FROM cdc.dbo_" + getReaderConfig().getTableName() + "_CT WHERE [__$start_lsn] BETWEEN @bglsn AND @edlsn");
            ParseUtil.parseSQLServerCDC(resultSet, getFieldNames(), syncData);
        } catch (SQLException | NullPointerException e) {
            logger.error(e);
        }
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
