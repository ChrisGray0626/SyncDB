package com.chris.syncData;

import com.chris.reader.ReaderTypeEnum;
import com.chris.util.ParseUtil;
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
    private Map<String, String> fieldNameMap;
    private List<String> rows;
    private Integer interval;
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

        writerType = WriterTypeEnum.valueOf(properties.getProperty("writer.writerType").toUpperCase());
        readerType = ReaderTypeEnum.valueOf(properties.getProperty("reader.readerType").toUpperCase());
        fieldsName = properties.getProperty("SyncData.fieldsName").replace(" ", "").split(",");
        interval = Integer.parseInt(properties.getProperty("SyncData.interval"));
        fieldNameMap = ParseUtil.parseFieldNameMap(properties.getProperty("SyncData.fieldNameMap"));
    }

    public void setRows(List<String> rows) {
        this.rows = rows;
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
        stringBuilder.append("rows: [");
        for (String row: rows) {
            stringBuilder.append(row).append(",");
        }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
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

    public List<String> getRows() {
        return rows;
    }

    public EventTypeEnum getEventType() {
        return eventType;
    }

    public WriterTypeEnum getWriterType() {
        return writerType;
    }

    public ReaderTypeEnum getReaderType() {
        return readerType;
    }

    public Integer getInterval() {
        return interval;
    }
}

