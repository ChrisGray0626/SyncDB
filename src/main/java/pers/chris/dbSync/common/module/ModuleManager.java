package pers.chris.dbSync.common.module;

import pers.chris.dbSync.syncData.SyncData;

public abstract class ModuleManager {

    public abstract void load();
    public abstract void checkRule();
    public abstract void parseRule();
    public abstract void run(SyncData syncData);

}
