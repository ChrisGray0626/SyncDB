package pers.chris.dbSync.syncData;

import pers.chris.dbSync.common.typeEnum.EventTypeEnum;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;


public class SyncData {
    public EventTypeEnum eventType;
    private Map<String, String> data;
    private final Logger logger = Logger.getLogger(SyncData.class);

    public SyncData() {
        data = new HashMap<>();
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

}