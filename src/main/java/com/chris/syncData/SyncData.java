package com.chris.syncData;

import com.chris.config.SyncDataConfig;
import com.chris.util.ParseUtil;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class SyncData {
    private EventTypeEnum eventType;
    private SyncDataConfig syncDataConfig;
    private String[] fieldsName;
    private Map<String, String> fieldNameMap;
    private String[] rows;
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

        fieldsName = properties.getProperty("SyncData.fieldsName").replace(" ", "").split(",");
        fieldNameMap = ParseUtil.parseFieldNameMap(properties.getProperty("SyncData.fieldNameMap"));
    }

    public void setRows(String[] rows) {
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

    public void setSyncDataConfig(SyncDataConfig syncDataConfig) {
        this.syncDataConfig = syncDataConfig;
    }

    public String[] getFieldsName() {
        return fieldsName;
    }

    public String[] getRows() {
        return rows;
    }

    public EventTypeEnum getEventType() {
        return eventType;
    }

    public SyncDataConfig getSyncDataConfig() {
        return syncDataConfig;
    }
}

