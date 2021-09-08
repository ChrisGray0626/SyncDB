package com.chris.reader;

import com.chris.syncData.SyncData;
import com.chris.util.ConnectUtil;
import com.chris.util.ParseUtil;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import common.DBTypeEnum;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.List;

public class MySQLReader extends AbstractReader {

    private String[] fieldsName;
    private Connection connection;
    private BinaryLogClient binlogClient; // binlog监听客户端
    private static final Logger logger = Logger.getLogger(MySQLReader.class);

    public MySQLReader() {
        dbType = DBTypeEnum.MYSQL;
    }

    @Override
    public void read(Integer interval) {
    }

    @Override
    public void read() {
    }

    @Override
    public void connect() {
        connection = ConnectUtil.connect(dbType, getReaderConfig().getUrl(), getReaderConfig().getUser(), getReaderConfig().getPassword());
        binlogClientConfig();
        binlogClientConnect();
    }

    // binlog监听客户端配置
    private void binlogClientConfig() {
        // 内部类使用需要，当前数据库名和表名
        final String[] curDataBaseName = {null};
        final String[] curTableName = {null};

        binlogClient = new BinaryLogClient(getReaderConfig().getHostname(), Integer.parseInt(getReaderConfig().getPort()), getReaderConfig().getUser(), getReaderConfig().getPassword());
        EventDeserializer eventDeserializer = new EventDeserializer();

        // 序列化格式设置
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
        );

        binlogClient.setEventDeserializer(eventDeserializer);

        binlogClient.registerEventListener(new BinaryLogClient.EventListener() {
            // 重写监听方法onEvent
            @Override
            public void onEvent(Event event) {
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
                        List<List<String>> rowsData = ParseUtil.parseMySQLBinLogRows(writeRowsEventData.getRows());

                        getSyncData().setEventType(SyncData.EventTypeEnum.INSERT);
                        for (List<String> rows: rowsData) {
                            getSyncData().setRows(rows);
                        }
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

    @Override
    public void close() {}
}
