package pers.chris.dbSync.conf;

import pers.chris.dbSync.common.typeEnum.SyncTypeEnum;

public class SyncConf {

    public SyncTypeEnum syncType;
    private Integer interval;
    private String timeField; // 记录时间的字段名

    public Integer getInterval() {
        return interval;
    }

    public void setInterval(Integer interval) {
        this.interval = interval;
    }

    public String getTimeField() {
        return timeField;
    }

    public void setTimeField(String timeField) {
        this.timeField = timeField;
    }
}
