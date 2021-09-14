package pers.chris.dbSync.fieldMapper;

import pers.chris.dbSync.util.FieldUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldMapManager {

    private final List<FieldMapper> fieldMappers;
    private List<String> rules;

    public FieldMapManager (List<String> rules) {
        fieldMappers = new ArrayList<>();
        this.rules = rules;
        configRules();
    }

    private void configRules() {
        for (String rule: rules) {
            fieldMappers.add(FieldUtil.parseRule(rule));
        }
    }

    public Map<String, String> run(Map<String, String> rows) {
        for (FieldMapper fieldMapper : fieldMappers) {
            fieldMapper.check();
            fieldMapper.map(rows);
        }
        return rows;
    }

    public List<String> getRules() {
        return rules;
    }

    public void setRules(List<String> rules) {
        this.rules = rules;
    }
}
