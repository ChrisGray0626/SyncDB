package pers.chris.dbSync.util;

import org.apache.log4j.Logger;
import pers.chris.dbSync.common.FieldTypeEnum;
import pers.chris.dbSync.exception.FieldMapException;
import pers.chris.dbSync.fieldMap.FieldMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class FieldUtil {

    private static final Logger logger = Logger.getLogger(FieldUtil.class);
    private static final Set<String> pointlessFields; // 记录某些数据库提供的无关字段
    private static final Map<String, FieldTypeEnum> fieldTypeMap;
    static {
        pointlessFields = new HashSet<>();
        pointlessFields.add("USER");
        pointlessFields.add("CURRENT_CONNECTIONS");
        pointlessFields.add("TOTAL_CONNECTIONS");

        fieldTypeMap = new HashMap<>();
        fieldTypeMap.put("INT", FieldTypeEnum.INT);
        fieldTypeMap.put("int4", FieldTypeEnum.INT);
        fieldTypeMap.put("serial", FieldTypeEnum.INT);
        fieldTypeMap.put("VARCHAR", FieldTypeEnum.STRING);
        fieldTypeMap.put("text", FieldTypeEnum.STRING);
        fieldTypeMap.put("TIMESTAMP", FieldTypeEnum.TIME);
    }

    public static Map<String, String> read(ResultSet resultSet) {
        // LinkedHashMap保证插入顺序
        Map<String, String> fields = new LinkedHashMap<>();

        try {
            while (resultSet.next()) {
                String field = resultSet.getString("COLUMN_NAME");
                String type = resultSet.getString("TYPE_NAME");
                // 排除不需要的无关字段
                if (pointlessFields.contains(field)) {
                    continue;
                }
                logger.debug(type);
                fields.put(field, type);
            }
        } catch (SQLException e) {
            logger.error(e);
        }
        return fields;
    }

    // 字段与值合并
    public static Map<String, String> mergeFieldAndValue(List<String> fields, List<String> values) {
        Map<String, String> rows = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            rows.put(fields.get(i), values.get(i));
        }
        return rows;
    }

    // TODO CheckField
    public static void check(List<String> fieldNames1, List<String> fieldNames2) throws FieldMapException {
        String fieldName1 = fieldNames1.get(0);
        String fieldName2 = fieldNames1.get(0);
        FieldTypeEnum fieldType1 = fieldTypeMap.get(fieldName1);
        FieldTypeEnum fieldType2 = fieldTypeMap.get(fieldName2);

        if (fieldType1 != fieldType2) {
            throw new FieldMapException(fieldName1 + "'s type is not adapted to " + fieldName2);
        }
    }

    // TODO Parse Map Rule
    public static FieldMap parseRule(String rules) {
        FieldMap fieldMap = new FieldMap();
        return fieldMap;
    }

}
