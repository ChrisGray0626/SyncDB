package pers.chris.dbSync.valueFilter;

import pers.chris.dbSync.common.module.ModuleManager;
import pers.chris.dbSync.conf.SyncConf;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.TimeUtil;

import java.util.Map;

public class ValueFilterManager extends ModuleManager {

    private final Map<String, ValueFilter> valueFilters;
    private SyncConf syncConf;
    private String filterSQL;

    public ValueFilterManager(Map<String, ValueFilter> valueFilters) {
        this.valueFilters = valueFilters;
    }

    @Override
    public void load() {
        parseRule();
    }

    @Override
    public void checkRule() {

    }

    @Override
    public void parseRule() {
        StringBuilder SQL = new StringBuilder();

        // 第0条规则,时间过滤
        SQL.append(timedFilterRule());
        for (ValueFilter valueFilter: valueFilters.values()) {
            SQL.append(" and ")
                    .append(valueFilter.getRule());
        }
        filterSQL = SQL.toString();
    }

    @Override
    public void run(SyncData syncData) {

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

    public String getFilterSQL() {
        return filterSQL;
    }

}
