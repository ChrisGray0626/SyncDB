package pers.chris.dbSync.valueFilter;

import java.util.ArrayList;
import java.util.List;

public class ValueFilterManager {

    private final List<ValueFilter> valueFilters;
    private List<String> rules;

    public ValueFilterManager (List<String> rules) {
        valueFilters = new ArrayList<>();
        this.rules = rules;
        configRules();
    }

    private void configRules() {
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
            SQL.append(" and ").append(valueFilter.getRule());
        }
        return SQL.toString();
    }

    public List<String> getRules() {
        return rules;
    }

    public void setRules(List<String> rules) {
        this.rules = rules;
    }

}
