package pers.chris.dbSync.util;

import org.apache.log4j.Logger;
import pers.chris.dbSync.fieldMap.FieldMap;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class FieldUtil {

    private static final Logger logger = Logger.getLogger(FieldUtil.class);
    private static final Set<String> pointlessFields; // 记录某些数据库提供的无关字段

    static {
        pointlessFields = new HashSet<>();
        pointlessFields.add("USER");
        pointlessFields.add("CURRENT_CONNECTIONS");
        pointlessFields.add("TOTAL_CONNECTIONS");
    }

    public static List<String> readFields(Connection connection, String tableName) {
        List<String> fieldsNames = new ArrayList<>();

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getColumns(null, "%", tableName, "%");

            while (resultSet.next()) {
                String field = resultSet.getString("COLUMN_NAME");
                // 排除不需要的无关字段
                if (pointlessFields.contains(field)) {
                    continue;
                }

                fieldsNames.add(field);
            }
        } catch (SQLException e) {
            logger.error(e);
        }
        return fieldsNames;
    }

    // 字段与值合并
    public static Map<String, String> mergeFieldAndValue(List<String> fields, List<String> values) {
        Map<String, String> rows = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            rows.put(fields.get(i), values.get(i));
        }
        return rows;
    }


    // TODO 解析字段映射规则
    public static FieldMap parseRule(String rules) {
        FieldMap fieldMap = new FieldMap();
        return fieldMap;
    }

}
