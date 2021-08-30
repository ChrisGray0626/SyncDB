package com.chris.syncData;

import com.chris.reader.ReaderTypeEnum;
import com.chris.writer.WriterTypeEnum;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class SyncData {
    private EventTypeEnum eventType;
    private ReaderTypeEnum readerType;
    private WriterTypeEnum writerType;
    private String databaseName;
    private String tableName;
    private String[] fieldsName;
    private List<List<String>> rowsData;
    private HashSet<String> targetTables;
    private SyncDataListener syncDataListener;
    private Logger logger = Logger.getLogger(SyncData.class);

    public SyncData(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public SyncData(){};

    public void config(String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            logger.error(e);
        }
        tableName = properties.getProperty("SyncData.tableName");
        fieldsName = properties.getProperty("SyncData.fieldsName").replace(" ", "").split(",");
    }

    public void setRowsData(List<List<String>> rowsData) {
        this.rowsData = rowsData;
        syncDataListener.doSet(new SyncDataEvent(this));
    }

    // TODO 库表筛选
    public boolean isTargetTable (String tableName) {
        return targetTables.contains(tableName);
    }

    public void registerListener(SyncDataListener syncDataListener) {
        this.syncDataListener = syncDataListener;
    }

    public interface SyncDataListener {
        void doSet(SyncDataEvent event);
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("tableName: ").append(tableName).append("\n");
        stringBuilder.append("rows: [\n");
        for (List<String> rows: rowsData) {
            stringBuilder.append("[");
            for (String row: rows) {
                stringBuilder.append(row).append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            stringBuilder.append("]\n");
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    public enum EventTypeEnum {
        INSERT,
        UPDATE,
        DELETE
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setFieldsName(String[] fieldsName) {
        this.fieldsName = fieldsName;
    }

    public void setTargetTables(HashSet<String> targetTables) {
        this.targetTables = targetTables;
    }

    public void setEventType(EventTypeEnum curEventTypeEnum) {
        this.eventType = curEventTypeEnum;
    }

    public void setReaderType(ReaderTypeEnum readerTypeEnum) {
        this.readerType = readerTypeEnum;
    }

    public void setWriterType(WriterTypeEnum writerTypeEnum) {
        this.writerType = writerTypeEnum;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String[] getFieldsName() {
        return fieldsName;
    }

    public List<List<String>> getRowsData() {
        return rowsData;
    }

    public EventTypeEnum getEventType() {
        return eventType;
    }

    public ReaderTypeEnum getReaderType() {
        return readerType;
    }

    public WriterTypeEnum getWriterType() {
        return writerType;
    }
}

