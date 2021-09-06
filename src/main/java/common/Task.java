package common;

import com.chris.syncData.SyncData;


public abstract class Task {

    public DBType dbType;
    private SyncData syncData;

    public abstract void connect();
    public abstract void close();

    public SyncData getSyncData() {
        return syncData;
    }

    public void setSyncData(SyncData syncData) {
        this.syncData = syncData;
    }
}
