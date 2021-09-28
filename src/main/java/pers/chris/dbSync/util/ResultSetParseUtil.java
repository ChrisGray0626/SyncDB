package pers.chris.dbSync.util;

import pers.chris.dbSync.common.typeEnum.DBTypeEnum;
import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.conf.SyncDataConf;
import pers.chris.dbSync.job.Job;
import pers.chris.dbSync.common.typeEnum.JobTypeEnum;
import pers.chris.dbSync.common.typeEnum.EventTypeEnum;
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

    private ResultSetParseUtil() {
    }

    private static final Logger logger = Logger.getLogger(Writer.class);

    public static Job parseJob(ResultSet resultSet) {
        Job job = new Job();

        try {
            job.setJobId(resultSet.getString("job_id"));
            job.jobType = JobTypeEnum.valueOf(resultSet.getString("job_type"));
            job.setSrcDBConfId(resultSet.getString("src_db_conf_id"));
            job.setDstDBConfId(resultSet.getString("dst_db_conf_id"));
            SyncDataConf syncDataConf = new SyncDataConf();
            syncDataConf.setInterval(Integer.parseInt(resultSet.getString("sync_interval")));
            syncDataConf.setTimeField(resultSet.getString("sync_time_field_name"));
            job.setSyncDataConf(syncDataConf);
        } catch (SQLException e) {
            logger.error(e);
        }


        return job;
    }

    public static List<Job> parseJobs(ResultSet resultSet) {
        List<Job> jobs = new ArrayList<>();

        try {
            while (resultSet.next()) {
                jobs.add(parseJob(resultSet));
            }
        }
        catch (SQLException e) {
            logger.error(e);
        }
        return jobs;
    }

    public static DBConf parseDBConf(ResultSet resultSet) {
        DBConf dbConf = new DBConf();
        try {
            resultSet.next();
            dbConf.dbType = DBTypeEnum.valueOf(resultSet.getString("db_type"));
            dbConf.setHostname(resultSet.getString("hostname"));
            dbConf.setPort(resultSet.getString("port"));
            dbConf.setDBName(resultSet.getString("db_name"));
            dbConf.setUser(resultSet.getString("user"));
            dbConf.setPassword(resultSet.getString("password"));
            dbConf.setTableName(resultSet.getString("table_name"));
        } catch (SQLException e) {
            logger.error(e);
        }
        return dbConf;
    }

    public static List<String> parseFieldMapConf(ResultSet resultSet) {
        List<String> rules = new ArrayList<>();
        try {
            while (resultSet.next()) {
                String rule = resultSet.getString("rule");
                rules.add(rule);
            }
        } catch (SQLException e) {
            logger.error(e);
        }
        return rules;
    }

    public static List<String> parseValueFilterConf(ResultSet resultSet) {
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

    // 常规（Pull方式）SQL解析
    public static void parseGeneralSQL(ResultSet resultSet, SyncData syncData, List<String> fieldNames) {
        try {
            while (resultSet.next()) {
                Map<String, String> data = new HashMap<>();

                // 根据字段名称获取对应数据
                for (String fieldName : fieldNames) {
                    data.put(fieldName, resultSet.getString(fieldName));
                }

                syncData.setEventType(EventTypeEnum.INSERT);
                syncData.setData(data);
                syncData.trigger();
            }
        }
        catch (SQLException e) {
            logger.error(e);
        }
    }

    public static List<List<String>> parseMySQLBinLogRows(List<Serializable[]> values) {
        List<List<String>> valuesData = new ArrayList<>();

        // 数据格式转换
        for (Serializable[] valuesArr : values) {
            List<String> list = new ArrayList<>();
            for (Serializable value : valuesArr) {
                if (value != null) {
                    // TODO 中文编码问题
                    if (value.equals("瀹氭椂鏁版嵁")) {
                        list.add("定时数据");
                    } else if (value.equals("澧為噺鏁版嵁")) {
                        list.add("增量数据");
                    } else {
                        list.add(value.toString());
                    }
                } else {
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
                        syncData.setData(FieldUtil.mergeFieldAndValue(fieldNames, values));
                        syncData.trigger();
                        break;
                    default:
                        break;
                }
            }
        }
        catch (SQLException e) {
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
                        for (String fieldName : fieldNames) {
                            rows.put(fieldName, resultSet.getString(fieldName));
                        }
                        syncData.setEventType(curEventType);
                        syncData.setData(rows);
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
