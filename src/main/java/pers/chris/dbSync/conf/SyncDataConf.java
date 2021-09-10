package pers.chris.dbSync.conf;

import java.util.ArrayList;
import java.util.List;

public class SyncDataConf {

    private Integer interval;
    private String timeField; // 记录时间的字段名
    private List<String> fieldMapRules;

    public SyncDataConf () {
        fieldMapRules = new ArrayList<>();
    }

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

    public List<String> getFieldMapRules() {
        return fieldMapRules;
    }

    public void setFieldMapRules(List<String> fieldMapRules) {
        this.fieldMapRules = fieldMapRules;
    }

}
