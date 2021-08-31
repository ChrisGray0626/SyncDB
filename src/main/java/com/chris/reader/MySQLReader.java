package com.chris.reader;

import com.chris.syncData.SyncData;
import com.chris.util.ParseUtil;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class MySQLReader extends AbstractReader {

    public ReaderTypeEnum readerType;
    private SyncData syncData;
    private String hostname;
    private String port;
    private String username;
    private String password;
    private String databaseName;
    private String tableName;
    private BinaryLogClient client; // binlog监听客户端

    public MySQLReader() {
        readerType = ReaderTypeEnum.MYSQL;
    }

    @Override
    public void config(String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        hostname = properties.getProperty("reader.hostname");
        port = properties.getProperty("reader.port");
        username = properties.getProperty("reader.username");
        password = properties.getProperty("reader.password");
        databaseName = properties.getProperty("reader.databaseName");
        tableName = properties.getProperty("reader.tableName");
    }

    @Override
    public void initSyncData(SyncData syncData) {
        this.syncData = syncData;
        syncData.setReaderType(readerType);
    }

    @Override
    public void read() {
    }

    // 监听客户端配置
    private void clientConfig() {
        // 内部类使用需要，当前数据库名和表名
        final String[] curDataBaseName = {null};
        final String[] curTableName = {null};

        client = new BinaryLogClient(hostname, Integer.parseInt(port), username, password);
        EventDeserializer eventDeserializer = new EventDeserializer();

        // 序列号格式设置
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
        );

        client.setEventDeserializer(eventDeserializer);

        client.registerEventListener(new BinaryLogClient.EventListener() {
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
                    if (databaseName.equals(curDataBaseName[0])
                    && tableName.equals(curTableName[0])) {
                        WriteRowsEventData writeRowsEventData = (WriteRowsEventData) eventData;

                        syncData.setEventType(SyncData.EventTypeEnum.INSERT);
                        syncData.setRowsData(ParseUtil.parseMySQLBinLogRows(writeRowsEventData.getRows()));
                    }
                }
            }
        });
    }

    @Override
    public void connect() {
        clientConfig();
        try {
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {}

    public void setSyncData(SyncData syncData) {
        this.syncData = syncData;
    }
}
