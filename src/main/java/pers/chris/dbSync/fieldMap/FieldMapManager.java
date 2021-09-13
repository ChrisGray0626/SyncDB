package pers.chris.dbSync.fieldMap;

import pers.chris.dbSync.util.FieldUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldMapManager {

    private List<FieldMap> fieldMappers;

    public FieldMapManager () {
        fieldMappers = new ArrayList<>();
    }

    public void configRules(List<String> fieldMapRules) {
        for (String fieldMapRule: fieldMapRules) {
            fieldMappers.add(FieldUtil.parseRule(fieldMapRule));
        }
    }

    public Map<String, String> run(Map<String, String> rows) {
        for (FieldMap fieldMap: fieldMappers) {
            fieldMap.check();
            fieldMap.map(rows);
        }
        return rows;
    }

    public List<FieldMap> getFieldMappers() {
        return fieldMappers;
    }

    public void setFieldMappers(List<FieldMap> fieldMappers) {
        this.fieldMappers = fieldMappers;
    }

}
