package pers.chris.dbSync.reader;

import pers.chris.dbSync.common.typeEnum.EventTypeEnum;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.FieldUtil;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import pers.chris.dbSync.common.typeEnum.DBTypeEnum;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MySQLReader extends Reader {

    private BinaryLogClient binlogClient; // binlog监听客户端
    private static final Logger logger = Logger.getLogger(MySQLReader.class);

    public MySQLReader() {
        dbType = DBTypeEnum.MYSQL;
    }

    @Override
    public void read() {
        binlogClientConfig();
        binlogClientConnect();
    }

    // binlog监听客户端配置
    private void binlogClientConfig() {
        // 内部类使用限制，当前数据库名和表名
        final String[] curDataBaseName = {null};
        final String[] curTableName = {null};

        binlogClient = new BinaryLogClient(getReaderConf().hostname, Integer.parseInt(getReaderConf().port), getReaderConf().user, getReaderConf().password);
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
                if (getReaderConf().dbName.equals(curDataBaseName[0])
                        && getReaderConf().tableName.equals(curTableName[0])) {
                    WriteRowsEventData writeRowsEventData = (WriteRowsEventData) eventData;
                    SyncData syncData = new SyncData();

                    syncData.eventType = EventTypeEnum.INSERT;

                    // 一次获取多条数据，数据格式为List<Serializable[]>
                    for (Serializable[] dataSerializable : writeRowsEventData.getRows()) {
                        List<String> data = new ArrayList<>();
                        for (Serializable valueSerializable : dataSerializable) {
                            String value = valueSerializable.toString();

                            // TODO 中文编码问题
                            if (value.equals("瀹氭椂鏁版嵁")) {
                                value = "定时数据";
                            }
                            if (value.equals("澧為噺鏁版嵁")) {
                                value = "增量数据";
                            }
                            data.add(value);
                        }
                        syncData.setData(FieldUtil.mergeFieldAndValue(getFieldNames(), data));
                        super.trigger(syncData);
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
