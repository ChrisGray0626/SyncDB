package pers.chris.dbSync.common;


import pers.chris.dbSync.syncData.SyncData;

public interface Readable {

    void read(SyncData syncData, Integer interval);

}
