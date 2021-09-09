package pers.chris.dbSync.common;


import pers.chris.dbSync.syncData.SyncData;

public interface Writeable {

    void write(SyncData syncData);
}
