package pers.chris.dbSync.syncData;

public class SyncDataEvent {

    private SyncData syncData;

    public SyncDataEvent(SyncData syncData) {
        this.syncData = syncData;
    }

    public SyncData getSyncData() {
        return syncData;
    }

    public void setSyncData(SyncData syncData) {
        this.syncData = syncData;
    }
}
