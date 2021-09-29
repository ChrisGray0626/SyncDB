package pers.chris.dbSync.reader;

import pers.chris.dbSync.common.typeEnum.EventTypeEnum;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.common.typeEnum.DBTypeEnum;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SQLServerReader extends Reader {

    private static final Logger logger = Logger.getLogger(SQLServerReader.class);

    public SQLServerReader() {
        dbType = DBTypeEnum.SQLSERVER;
    }

    @Override
    public void read() {
        Integer interval = super.getJobConf().getInterval();

        while (true) {
            readCDCTable(interval);
            try {
                TimeUnit.MINUTES.sleep(interval);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    // 读取指定表的CDC表
    private void readCDCTable (Integer interval) {

        try {
            // 查询最近 interval 时间（分钟）内的捕获数据
            Statement statement = getConnection().createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "DECLARE @bglsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('smallest greater than or equal',DATEADD(mi,-" + interval + ",GETDATE()));"
                    + "DECLARE @edlsn VARBINARY(10)=sys.fn_cdc_map_time_to_lsn('largest less than or equal',GETDATE());"
                    + "SELECT * FROM cdc.dbo_" + getReaderConf().getTableName() + "_CT "
                            + "WHERE [__$start_lsn] BETWEEN @bglsn AND @edlsn");

            while (resultSet.next()) {
                SyncData syncData = new SyncData();
                EventTypeEnum eventType = getSQLServerEventType(resultSet.getString("__$operation"));

                syncData.eventType = eventType;
                switch (eventType) {
                    case INSERT:
                        Map<String, String> rows = new HashMap<>();

                        // 根据字段名称获取对应数据
                        for (String fieldName : getFieldNames()) {
                            rows.put(fieldName, resultSet.getString(fieldName));
                        }
                        syncData.setData(rows);
                        super.trigger(syncData);
                        break;
                    default:
                }
            }
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    // SQLServer内根据字段__$operation获取事件类型
    private EventTypeEnum getSQLServerEventType(String __$operation) {
        EventTypeEnum eventType = null;
        switch (__$operation) {
            case "1":
                eventType = EventTypeEnum.DELETE;
                break;
            case "2":
                eventType = EventTypeEnum.INSERT;
                break;
            case "3":
                eventType = EventTypeEnum.UPDATE;
                break;
            case "4":
                eventType = EventTypeEnum.UPDATE;
                break;
            default:
                break;
        }
        return eventType;
    }

}
