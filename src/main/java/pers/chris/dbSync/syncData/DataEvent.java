package pers.chris.dbSync.syncData;

public class DataEvent {

    private final SyncData syncData;

    public DataEvent(SyncData syncData) {
        this.syncData = syncData;
    }

    public SyncData getSyncData() {
        return syncData;
    }
}
