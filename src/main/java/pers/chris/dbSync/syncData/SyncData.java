package pers.chris.dbSync.syncData;

import pers.chris.dbSync.conf.SyncDataConf;
import org.apache.log4j.Logger;
import pers.chris.dbSync.mapper.FieldMapperManager;

import java.util.HashMap;
import java.util.Map;


public class SyncData {
    private EventTypeEnum eventType;
    private SyncDataConf syncDataConf;
    // TODO fieldMapperManager
    private FieldMapperManager fieldMapperManager;
    private Map<String, String> rows;
    private SyncDataListener syncDataListener;
    private final Logger logger = Logger.getLogger(SyncData.class);

    public SyncData() {
        rows = new HashMap<>();
    }

    public void setRows(Map<String, String> rows) {
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
        for (Map.Entry entry: rows.entrySet()) {
            stringBuilder.append(entry.getValue()).append(",");
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

    public EventTypeEnum getEventType() {
        return eventType;
    }

    public void setEventType(EventTypeEnum curEventTypeEnum) {
        this.eventType = curEventTypeEnum;
    }

    public SyncDataConf getSyncDataConfig() {
        return syncDataConf;
    }

    public void setSyncDataConfig(SyncDataConf syncDataConf) {
        this.syncDataConf = syncDataConf;
    }

    public Map<String, String> getRows() {
        return rows;
    }
}

