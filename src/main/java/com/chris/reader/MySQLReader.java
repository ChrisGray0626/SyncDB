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

    public ReaderTypeEnum readerType = ReaderTypeEnum.MYSQL;
    private SyncData syncData;
    private String hostname;
    private String port;
    private String username;
    private String password;
    private BinaryLogClient client;

    public MySQLReader() {
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
    }


    @Override
    public void init(SyncData syncData) {
        this.syncData = syncData;
        syncData.setReaderType(readerType);
    }


    @Override
    public void read() {
    }

    private void binlogListener() {
        client = new BinaryLogClient(hostname, Integer.parseInt(port), username, password);
        EventDeserializer eventDeserializer = new EventDeserializer();

        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
        );

        client.setEventDeserializer(eventDeserializer);
        client.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                EventData eventData = event.getData();

                if (eventData instanceof TableMapEventData) {
                    TableMapEventData tableMapEventData = (TableMapEventData) eventData;
                    syncData.setDatabaseName(tableMapEventData.getDatabase());
                    syncData.setTableName(tableMapEventData.getTable());
                } else if (eventData instanceof WriteRowsEventData) {
                    WriteRowsEventData writeRowsEventData = (WriteRowsEventData) eventData;
                    syncData.setEventType(SyncData.EventTypeEnum.INSERT);
                    syncData.setRowsData(ParseUtil.parseMySQLBinLogRows(writeRowsEventData.getRows()));
                }
            }
        });
    }

    @Override
    public void connect() {
        binlogListener();
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
