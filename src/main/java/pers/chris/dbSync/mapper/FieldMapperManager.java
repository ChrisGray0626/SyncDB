package pers.chris.dbSync.mapper;

import java.util.List;
import java.util.Map;

public class FieldMapperManager {

    private List<FieldMapper> fieldMappers;

    public Map<String, String> run (Map<String, String> rows) {
        for (FieldMapper fieldMapper: fieldMappers) {
            if (fieldMapper.getDstFields().size() == 1) {
                rows = multi2One(fieldMapper, rows);
            }
        }
        return rows;
    }

    public Map<String, String> multi2One(FieldMapper fieldMapper, Map<String, String> rows) {
        List<String> srcFields = fieldMapper.getSrcFields();
        String dstField = fieldMapper.getDstFields().get(0);
        StringBuilder dstValue = new StringBuilder();

        for (String field: srcFields) {
            String value = rows.get(field);
            dstValue.append(value);
            rows.remove(field);
        }

        rows.put(dstField, dstValue.toString());
        return rows;
    }

    public List<FieldMapper> getFieldMappers() {
        return fieldMappers;
    }

    public void setFieldMappers(List<FieldMapper> fieldMappers) {
        this.fieldMappers = fieldMappers;
    }
}
