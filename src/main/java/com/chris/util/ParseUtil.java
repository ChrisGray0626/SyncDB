package com.chris.util;

import com.chris.syncData.SyncData;
import com.chris.writer.PostgreSQLWriter;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseUtil {

    private static final Logger logger = Logger.getLogger(PostgreSQLWriter.class);

    public static void parsePGLogicalSlot(ResultSet resultSet, SyncData syncData) {
        List<List<String>> rowsData = new ArrayList<>();
        try {
            while (resultSet.next()) {
                String[] originalData = resultSet.getString("data").split(" ");
                if (!"table".equals(originalData[0])) {
                    continue;
                }

                String[] originalData1 = originalData[1].split("\\.");
                String schema = originalData1[0];
                String tableName = originalData1[1].replace(":", "");
                SyncData.EventTypeEnum eventType = SyncData.EventTypeEnum.valueOf(originalData[2].replace(":", ""));

                if (eventType.equals(SyncData.EventTypeEnum.INSERT)) {
                    Pattern pattern = Pattern.compile("(?<=:)(('.*')|([\\S]+))");
                    Matcher matcher = pattern.matcher(resultSet.getString("data"));
                    List<String> rows = new ArrayList<>();
                    while (matcher.find()) {
                        rows.add(matcher.group().replace("'", ""));
                    }
                    rowsData.add(rows);
                }
                syncData.setTableName(tableName);
                syncData.setEventType(eventType);
                syncData.setRowsData(rowsData);
            }
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public static List<List<String>> parseMySQLBinLogRows(List<Serializable[]> rows) {
        List<List<String>> rowsData = new ArrayList<>();
        for (Serializable[] rowsArr: rows) {
            List<String> list = new ArrayList<>();
            for (Serializable row: rowsArr) {
                if (row != null) {
                    // TODO 中文编码问题
                    // 临时处理
                    if (row.equals("瀹氭椂鏁版嵁")) {
                        list.add("定时数据");
                    }
                    else if (row.equals("澧為噺鏁版嵁")) {
                        list.add("增量数据");
                    }
                    else {
                        list.add(row.toString());
                    }
                }
                else {
                    list.add(null);
                }
            }
            rowsData.add(list);
        }
        return rowsData;
    }
}
