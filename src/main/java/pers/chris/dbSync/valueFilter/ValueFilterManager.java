package pers.chris.dbSync.valueFilter;

import pers.chris.dbSync.conf.SyncConf;
import pers.chris.dbSync.util.TimeUtil;

import java.util.Map;

public class ValueFilterManager {

    private final Map<String, ValueFilter> valueFilters;
    private SyncConf syncConf;

    public ValueFilterManager(Map<String, ValueFilter> valueFilters) {
        this.valueFilters = valueFilters;
    }

    public String run() {
        StringBuilder SQL = new StringBuilder();

        // 第0条特殊规则
        SQL.append(timedFilterRule());
        for (ValueFilter valueFilter: valueFilters.values()) {
            SQL.append(" and ")
                    .append(valueFilter.getRule());
        }
        return SQL.toString();
    }

    // 定时过滤
    private String timedFilterRule() {
        return syncConf.timeField
                + ">="
                + TimeUtil.intervalTime(syncConf.interval);
    }

    public void setSyncConf(SyncConf syncConf) {
        this.syncConf = syncConf;
    }
}
