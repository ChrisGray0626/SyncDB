package pers.chris.dbSync.syncData;

import pers.chris.dbSync.conf.SyncDataConf;
import org.apache.log4j.Logger;
import pers.chris.dbSync.fieldMap.FieldMapManager;

import java.util.HashMap;
import java.util.Map;


public class SyncData {
    private EventTypeEnum eventType;
    private SyncDataConf syncDataConf;
    private Map<String, String> rows;
    private Map<String, String> writeFields;
    private Map<String, String> readFields;
    private FieldMapManager fieldMapManager;
    private SyncDataListener syncDataListener;
    private final Logger logger = Logger.getLogger(SyncData.class);

    public SyncData() {
        rows = new HashMap<>();
    }

    public void run(Map<String, String> rows) {
        this.rows = rows;
        fieldMap();

        syncDataListener.doSet(new SyncDataEvent(this));
    }

    // 字段映射
    private void fieldMap () {
        fieldMapManager.ensureField(writeFields, readFields);
        rows = fieldMapManager.run(rows);
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

    public void setRows(Map<String, String> rows) {
        this.rows = rows;
    }

    public Map<String, String> getWriteFields() {
        return writeFields;
    }

    public void setWriteFields(Map<String, String> writeFields) {
        this.writeFields = writeFields;
    }

    public Map<String, String> getReadFields() {
        return readFields;
    }

    public void setReadFields(Map<String, String> readFields) {
        this.readFields = readFields;
    }

    public FieldMapManager getFieldMapManager() {
        return fieldMapManager;
    }

    public void setFieldMapManager(FieldMapManager fieldMapManager) {
        this.fieldMapManager = fieldMapManager;
    }

}

