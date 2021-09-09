package pers.chris.dbSync.util;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldUtil {

    private static final Logger logger = Logger.getLogger(FieldUtil.class);

    public static List<String> readFields(Connection connection, String tableName) {
        List<String> fieldsNames = new ArrayList<>();

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getColumns(null, "%", tableName, "%");

            while (resultSet.next()) {
                fieldsNames.add(resultSet.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            logger.error(e);
        }
        return fieldsNames;
    }

    // 字段与数据合并
    public static Map<String, String> list2Map (List<String> fields, List<String> values) {
        Map<String, String> rows = new HashMap<>();

        for (int i = 0; i < fields.size(); i++) {
            rows.put(fields.get(i), values.get(i));
        }

        return rows;
    }
}
