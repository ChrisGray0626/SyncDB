package com.chris.syncData;

import com.chris.reader.ReaderTypeEnum;
import com.chris.writer.WriterTypeEnum;

import java.util.List;

public class SyncData {
    private EventTypeEnum eventType;
    private ReaderTypeEnum readerType;
    private WriterTypeEnum writerType;
    private String databaseName;
    private String tableName;
    private List<List<String>> rowsData;
    private SyncDataListener syncDataListener;

    public SyncData(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public SyncData(){};

    public void setRowsData(List<List<String>> rowsData) {
        this.rowsData = rowsData;
        syncDataListener.doSet(new SyncDataEvent(this));
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

