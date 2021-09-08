package common;

import com.chris.syncData.SyncData;


public abstract class Task {

    public DBTypeEnum dbType;
    private SyncData syncData;
    private String[] fieldNames;

    public abstract void connect();
    public abstract void close();

    public SyncData getSyncData() {
        return syncData;
    }

    public void setSyncData(SyncData syncData) {
        this.syncData = syncData;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }
}
