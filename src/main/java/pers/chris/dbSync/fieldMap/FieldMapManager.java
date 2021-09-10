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

    public Map<String, String> map(Map<String, String> rows) {
        for (FieldMap fieldMap: fieldMappers) {
            if (fieldMap.getDstFields().size() == 1) {
                rows = multi2One(fieldMap, rows);
            }
        }
        return rows;
    }

    public Map<String, String> multi2One(FieldMap fieldMap, Map<String, String> rows) {
        List<String> srcFields = fieldMap.getSrcFields();
        String dstField = fieldMap.getDstFields().get(0);
        StringBuilder dstValue = new StringBuilder();

        for (String field: srcFields) {
            String value = rows.get(field);
            dstValue.append(value);
            rows.remove(field);
        }

        rows.put(dstField, dstValue.toString());
        return rows;
    }

    public List<FieldMap> getFieldMappers() {
        return fieldMappers;
    }

    public void setFieldMappers(List<FieldMap> fieldMappers) {
        this.fieldMappers = fieldMappers;
    }

}
