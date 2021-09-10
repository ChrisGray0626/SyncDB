package pers.chris.dbSync.util;

import java.util.Map;

public class SQLUtil {

    public static String insertSQL(String tableName, Map<String, String> rows) {
        StringBuilder fields = new StringBuilder();
        StringBuilder values = new StringBuilder();

        fields.append("(");
        values.append("(");
        for (Map.Entry<String, String> entry: rows.entrySet()) {
            fields.append(entry.getKey()).append(",");
            values.append("'").append(entry.getValue()).append("',");
        }
        fields.deleteCharAt(fields.length() - 1);
        values.deleteCharAt(values.length() - 1);
        fields.append(")");
        values.append(")");
        return "INSERT INTO " + tableName + fields + " VALUES" + values;
    }
}
