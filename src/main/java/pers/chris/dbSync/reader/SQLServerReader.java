package pers.chris.dbSync.reader;

import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.ResultSetParseUtil;
import pers.chris.dbSync.common.DBTypeEnum;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.concurrent.TimeUnit;

public class SQLServerReader extends Reader {

    private static final Logger logger = Logger.getLogger(SQLServerReader.class);

    public SQLServerReader() {
        dbType = DBTypeEnum.SQLSERVER;
    }

    @Override
    public void read(SyncData syncData, Integer interval) {
        while (true) {
            readCDCTable(syncData, interval);
            try {
                TimeUnit.MINUTES.sleep(interval);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    // 读取指定表的CDC表
    private void readCDCTable (SyncData syncData, Integer interval) {
        try {
            // 查询最近 interval 时间（分钟）内的捕获数据
            Connection connection = getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "DECLARE @bglsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('smallest greater than or equal',DATEADD(mi,-" + interval + ",GETDATE()));"
                    + "DECLARE @edlsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('largest less than or equal',GETDATE());"
                    + "SELECT * FROM cdc.dbo_" + getReaderConfig().getTableName() + "_CT "
                            + "WHERE [__$start_lsn] BETWEEN @bglsn AND @edlsn");
            ResultSetParseUtil.parseSQLServerCDC(resultSet, getFieldNames(), syncData);
        } catch (SQLException | NullPointerException e) {
            logger.error(e);
        }
    }

}
