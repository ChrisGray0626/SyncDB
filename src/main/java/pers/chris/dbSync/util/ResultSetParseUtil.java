package pers.chris.dbSync.util;

import pers.chris.dbSync.common.DBTypeEnum;
import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.conf.SyncDataConf;
import pers.chris.dbSync.job.Job;
import pers.chris.dbSync.job.JobTypeEnum;
import pers.chris.dbSync.syncData.EventTypeEnum;
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


public class ResultSetParseUtil {

    private static final Logger logger = Logger.getLogger(Writer.class);

    public static List<Job> parseJobConf(ResultSet resultSet) {
        List<Job> jobs = new ArrayList<>();

        try {
            while (resultSet.next()) {
                Job job = new Job();
                SyncDataConf syncDataConf = new SyncDataConf();
                DBConf writerConf = new DBConf();
                DBConf readerConf = new DBConf();

                job.setJobId(resultSet.getString("job_id"));
                job.jobType = JobTypeEnum.valueOf(resultSet.getString("job_type"));

                syncDataConf.setInterval(Integer.parseInt(resultSet.getString("sync_interval")));
                syncDataConf.setTimeField(resultSet.getString("sync_time_field_name"));
                List<String> fieldMapRules = new ArrayList<>();
                writerConf.dbType = DBTypeEnum.valueOf(resultSet.getString("writer_db_type"));
                writerConf.setHostname(resultSet.getString("writer_hostname"));
                writerConf.setPort(resultSet.getString("writer_port"));
                writerConf.setDBName(resultSet.getString("writer_db_name"));
                writerConf.setUser(resultSet.getString("writer_user"));
                writerConf.setPassword(resultSet.getString("writer_password"));
                writerConf.setTableName(resultSet.getString("writer_table_name"));
                readerConf.dbType = DBTypeEnum.valueOf(resultSet.getString("reader_db_type"));
                readerConf.setHostname(resultSet.getString("reader_hostname"));
                readerConf.setPort(resultSet.getString("reader_port"));
                readerConf.setDBName(resultSet.getString("reader_db_name"));
                readerConf.setUser(resultSet.getString("reader_user"));
                readerConf.setPassword(resultSet.getString("reader_password"));
                readerConf.setTableName(resultSet.getString("reader_table_name"));

                job.setSyncDataConf(syncDataConf);
                job.setWriterConf(writerConf);
                job.setReaderConf(readerConf);

                jobs.add(job);
            }
        }
        catch (SQLException e) {
            logger.error(e);
        }
        return jobs;
    }

    public static List<String> parseFieldMapConf (ResultSet resultSet) {
        List<String> rules = new ArrayList<>();
        try {
            while (resultSet.next()) {
                String rule = resultSet.getString("rule");
                rules.add(rule);
            }
        }
        catch (SQLException e) {
            logger.error(e);
        }
        return rules;
    }

    public static List<String> parseValueFilterConf (ResultSet resultSet) {
        List<String> rules = new ArrayList<>();
        try {
            while (resultSet.next()) {
                String rule = resultSet.getString("rule");
                rules.add(rule);
            }
        }
        catch (SQLException e) {
            logger.error(e);
        }
        return rules;
    }

    // 解析常规（Pull方式）SQL
    public static void parseGeneralSQL(ResultSet resultSet, SyncData syncData, List<String> fieldNames) {
        try {
            while (resultSet.next()) {
                Map<String, String> rows = new HashMap<>();

                // 根据字段名称获取对应数据
                for (String fieldName: fieldNames) {
                    rows.put(fieldName, resultSet.getString(fieldName));
                }

                syncData.setEventType(EventTypeEnum.INSERT);
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
                EventTypeEnum eventType = EventTypeEnum.valueOf(originalData[2].replace(":", ""));

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
                        syncData.setRows(FieldUtil.mergeFieldAndValue(fieldNames, values));
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
                EventTypeEnum curEventType = getSQLServerEventType(resultSet.getString("__$operation"));

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
    private static EventTypeEnum getSQLServerEventType(String __$operation) {
        EventTypeEnum eventType = null;
        switch (__$operation) {
            case "1":
                eventType = EventTypeEnum.DELETE;
                break;
            case "2":
                eventType = EventTypeEnum.INSERT;
                break;
            case "3":
                eventType = EventTypeEnum.UPDATE;
                break;
            case "4":
                eventType = EventTypeEnum.UPDATE;
                break;
            default:
                break;
        }
        return eventType;
    }

}
