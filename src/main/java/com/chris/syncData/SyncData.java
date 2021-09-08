package com.chris.syncData;

import com.chris.configuration.SyncDataConfiguration;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;


public class SyncData {
    private EventTypeEnum eventType;
    private SyncDataConfiguration syncDataConfiguration;
    private List<String> rows;
    private SyncDataListener syncDataListener;
    private final Logger logger = Logger.getLogger(SyncData.class);

    public SyncData() {
        rows = new ArrayList<>();
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

    public void setSyncDataConfig(SyncDataConfiguration syncDataConfiguration) {
        this.syncDataConfiguration = syncDataConfiguration;
    }

    public List<String> getRows() {
        return rows;
    }

    public EventTypeEnum getEventType() {
        return eventType;
    }

    public SyncDataConfiguration getSyncDataConfig() {
        return syncDataConfiguration;
    }
}

