package com.chris.syncData;

import com.chris.reader.ReaderTypeEnum;
import com.chris.writer.WriterTypeEnum;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class SyncData {
    private EventTypeEnum eventType;
    private WriterTypeEnum writerType;
    private ReaderTypeEnum readerType;
    private String[] fieldsName;
    // TODO 数据格式修正
    private List<List<String>> rowsData;
    private SyncDataListener syncDataListener;
    private final Logger logger = Logger.getLogger(SyncData.class);

    public SyncData(){};

    public void config(String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            logger.error(e);
        }

        writerType = WriterTypeEnum.valueOf(properties.getProperty("writer.writerType"));
        readerType = ReaderTypeEnum.valueOf(properties.getProperty("reader.readerType"));
        fieldsName = properties.getProperty("SyncData.fieldsName").replace(" ", "").split(",");
    }

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

    public void setEventType(EventTypeEnum curEventTypeEnum) {
        this.eventType = curEventTypeEnum;
    }

    public void setReaderType(ReaderTypeEnum readerTypeEnum) {
        this.readerType = readerTypeEnum;
    }

    public void setWriterType(WriterTypeEnum writerTypeEnum) {
        this.writerType = writerTypeEnum;
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

    public String getWriterType() {
        return writerType.toString();
    }

    public String getReaderType() {
        return readerType.toString();
    }
}

