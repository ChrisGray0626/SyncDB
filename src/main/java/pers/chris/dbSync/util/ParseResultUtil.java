package pers.chris.dbSync.util;

import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.writer.Writer;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static pers.chris.dbSync.syncData.SyncData.EventTypeEnum.INSERT;

public class ParseResultUtil {

    private static final Logger logger = Logger.getLogger(Writer.class);

    public static void parseSQL(ResultSet resultSet, SyncData syncData, List<String> fieldNames) {
        try {
            while (resultSet.next()) {
                Map<String, String> rows = new HashMap<>();

                // 根据字段名称获取对应数据
                for (String fieldName: fieldNames) {
                    rows.put(fieldName, resultSet.getString(fieldName));
                }

                syncData.setEventType(INSERT);
                syncData.setRows(rows);
            }
        }
        catch (SQLException e) {
            logger.error(e);
        }
    }

    public static List<List<String>> parseMySQLBinLogRows(List<Serializable[]> values) {
        List<List<String>> valuesData = new ArrayList<>();

        // 数据格式转换
        for (Serializable[] valuesArr: values) {
            List<String> list = new ArrayList<>();
            for (Serializable value: valuesArr) {
                if (value != null) {
                    // TODO 中文编码问题
                    // 临时处理中文乱码问题
                    if (value.equals("瀹氭椂鏁版嵁")) {
                        list.add("定时数据");
                    }
                    else if (value.equals("澧為噺鏁版嵁")) {
                        list.add("增量数据");
                    }
                    else {
                        list.add(value.toString());
                    }
                }
                else {
                    list.add("");
                }
            }
            valuesData.add(list);
        }
        return valuesData;
    }

    public static void parsePGSQLLogicalSlot(ResultSet resultSet, SyncData syncData, String tableName, List<String> fieldNames) {
        try {
            while (resultSet.next()) {
                // 获取原始数据
                String[] originalData = resultSet.getString("data").split(" ");
                // 非table开头的不是事件数据
                if (!"table".equals(originalData[0])) {
                    continue;
                }

                // 解析原始数据
                String[] originalData1 = originalData[1].split("\\.");
                String schema = originalData1[0];
                String curTableName = originalData1[1].replace(":", "");
                SyncData.EventTypeEnum eventType = SyncData.EventTypeEnum.valueOf(originalData[2].replace(":", ""));

                if (!curTableName.equals(tableName)) {
                    continue;
                }

                switch (eventType) {
                    case INSERT:
                        List<String> values = new ArrayList<>();
                        // 正则匹配数据
                        Pattern pattern = Pattern.compile("(?<=:)(('.*?')|([\\S]+))");
                        Matcher matcher = pattern.matcher(resultSet.getString("data"));
                        while (matcher.find()) {
                            values.add(matcher.group().replace("'", ""));
                        }
                        syncData.setEventType(eventType);
                        syncData.setRows(FieldUtil.list2Map(fieldNames, values));
                        break;
                    default:
                        break;
                }
            }
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public static void parseSQLServerCDC(ResultSet resultSet, List<String> fieldNames, SyncData syncData) {
        try {
            while (resultSet.next()) {
                SyncData.EventTypeEnum curEventType = getSQLServerEventType(resultSet.getString("__$operation"));

                switch (curEventType) {
                    case INSERT:
                        Map<String, String> rows = new HashMap<>();

                        // 根据字段名称获取对应数据
                        for (String fieldName: fieldNames) {
                            rows.put(fieldName, resultSet.getString(fieldName));
                        }
                        syncData.setEventType(curEventType);
                        syncData.setRows(rows);
                        break;
                    default:
                        break;
                }
            }
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    // SQLServer内根据字段__$operation获取事件类型
    private static SyncData.EventTypeEnum getSQLServerEventType(String __$operation) {
        SyncData.EventTypeEnum eventType = null;
        switch (__$operation) {
            case "1":
                eventType = SyncData.EventTypeEnum.DELETE;
                break;
            case "2":
                eventType = INSERT;
                break;
            case "3":
                eventType = SyncData.EventTypeEnum.UPDATE;
                break;
            case "4":
                eventType = SyncData.EventTypeEnum.UPDATE;
                break;
            default:
                break;
        }
        return eventType;
    }

    // 解析字段映射关系
    public static Map<String, String> parseFieldNameMap(String ss) {
        Map<String, String> fieldNameMap = new HashMap<>();
        String[] maps = ss.split(";");
        for (String map: maps) {
            String srcFieldName = map.split(":")[0];
            String dstFieldName = map.split(":")[1];
            fieldNameMap.put(srcFieldName, dstFieldName);
        }
        return fieldNameMap;
    }
}
