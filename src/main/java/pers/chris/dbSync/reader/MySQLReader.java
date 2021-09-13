package pers.chris.dbSync.reader;

import pers.chris.dbSync.syncData.EventTypeEnum;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.FieldUtil;
import pers.chris.dbSync.util.ResultSetParseUtil;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import pers.chris.dbSync.common.DBTypeEnum;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class MySQLReader extends Reader {

    private BinaryLogClient binlogClient; // binlog监听客户端
    private static final Logger logger = Logger.getLogger(MySQLReader.class);

    public MySQLReader() {
        dbType = DBTypeEnum.MYSQL;
    }

    @Override
    public void read(SyncData syncData, Integer interval) {
        binlogClientConfig(syncData);
        binlogClientConnect();
    }

    // binlog监听客户端配置
    private void binlogClientConfig(SyncData syncData) {
        // 内部类使用限制，当前数据库名和表名
        final String[] curDataBaseName = {null};
        final String[] curTableName = {null};

        binlogClient = new BinaryLogClient(getReaderConfig().getHostname(), Integer.parseInt(getReaderConfig().getPort()), getReaderConfig().getUser(), getReaderConfig().getPassword());
        EventDeserializer eventDeserializer = new EventDeserializer();

        // 序列化格式设置
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
        );

        binlogClient.setEventDeserializer(eventDeserializer);

        // 重写监听方法onEvent
        binlogClient.registerEventListener(event -> {
            EventData eventData = event.getData();

            // 获取数据库、表信息
            if (eventData instanceof TableMapEventData) {
                TableMapEventData tableMapEventData = (TableMapEventData) eventData;

                curDataBaseName[0] = tableMapEventData.getDatabase();
                curTableName[0] = tableMapEventData.getTable();
            }
            // 获取事件类型INSERT的数据
            else if (eventData instanceof WriteRowsEventData) {
                if (getReaderConfig().getDBName().equals(curDataBaseName[0])
                        && getReaderConfig().getTableName().equals(curTableName[0])) {
                    WriteRowsEventData writeRowsEventData = (WriteRowsEventData) eventData;

                    List<List<String>> valuesData = ResultSetParseUtil.parseMySQLBinLogRows(writeRowsEventData.getRows());

                    syncData.setEventType(EventTypeEnum.INSERT);
                    for (List<String> values: valuesData) {
                        syncData.setRows(FieldUtil.mergeFieldAndValue(getFieldNames(), values));
                    }
                }
            }
        });
    }

    private void binlogClientConnect() {
        try {
            binlogClient.connect();
        } catch (IOException e) {
            logger.error(e);
        }
    }

}
