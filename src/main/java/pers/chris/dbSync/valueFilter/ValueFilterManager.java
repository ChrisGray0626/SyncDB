package pers.chris.dbSync.valueFilter;

import java.util.ArrayList;
import java.util.List;

public class ValueFilterManager {

    private final List<ValueFilter> valueFilters;

    public ValueFilterManager (List<String> rules) {
        valueFilters = new ArrayList<>();
        load(rules.toArray(new String[0]));
    }

    public void add(String rule) {
        load(rule);
    }

    public void addAll(List<String> rules) {
        load(rules.toArray(new String[0]));
    }

    private void load(String... rules) {
        for (String rule: rules) {
            valueFilters.add(new ValueFilter(rule));
        }
    }

    public String filterSQL() {
        if (valueFilters.isEmpty()) {
            return "";
        }

        StringBuilder SQL = new StringBuilder();
        for (ValueFilter valueFilter: valueFilters) {
            SQL.append(valueFilter.getRule())
                    .append(" and ");
        }
        return SQL.toString();
    }

}
