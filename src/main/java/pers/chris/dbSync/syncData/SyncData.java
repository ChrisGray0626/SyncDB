package pers.chris.dbSync.syncData;

import pers.chris.dbSync.common.ProcedureEvent;
import pers.chris.dbSync.common.typeEnum.EventTypeEnum;
import pers.chris.dbSync.conf.SyncDataConf;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


public class SyncData {
    private EventTypeEnum eventType;
    private SyncDataConf syncDataConf;
    private Map<String, String> data;
    private Map<String, String> writeFields;
    private Map<String, String> readFields;
    private SyncDataListener syncDataListener;
    private final Logger logger = Logger.getLogger(SyncData.class);

    public SyncData() {
        data = new HashMap<>();
    }

    // 触发后续操作
    public void trigger() {
        syncDataListener.doSet(new ProcedureEvent());
    }

    public void registerListener(SyncDataListener syncDataListener) {
        this.syncDataListener = syncDataListener;
    }

    public interface SyncDataListener {
        void doSet(ProcedureEvent event);
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

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
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

}