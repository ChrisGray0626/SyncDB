package pers.chris.dbSync.conf;

import pers.chris.dbSync.common.typeEnum.SyncTypeEnum;

public class SyncConf {

    public SyncTypeEnum syncType;
    public Integer interval;
    public String timeField; // 记录时间的字段名

}
