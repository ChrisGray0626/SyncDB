package pers.chris.dbSync.util;

import java.util.List;
import java.util.Map;

public class GenerateSQLUtil {

    public static String insertSQL(String tableName, Map<String, String> rows) {
        StringBuilder values = new StringBuilder();

        for (Map.Entry entry: rows.entrySet()) {
            values.append("'").append(entry.getValue()).append("',");
        }
        values.deleteCharAt(values.length() - 1);

        return "INSERT INTO " + tableName + " VALUES(" + values + ")";
    }
}
