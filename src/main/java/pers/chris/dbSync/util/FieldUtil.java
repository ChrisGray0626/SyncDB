package pers.chris.dbSync.util;

import org.apache.log4j.Logger;
import pers.chris.dbSync.common.FieldTypeEnum;
import pers.chris.dbSync.exception.FieldMapException;
import pers.chris.dbSync.fieldMapper.FieldMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldUtil {

    private FieldUtil() {
    }

    private static final Logger logger = Logger.getLogger(FieldUtil.class);
    private static final Set<String> pointlessFields; // 记录某些数据库提供的无关字段
    private static final Map<String, FieldTypeEnum> fieldTypeMap;

    static {
        pointlessFields = new HashSet<>();
        pointlessFields.add("USER");
        pointlessFields.add("CURRENT_CONNECTIONS");
        pointlessFields.add("TOTAL_CONNECTIONS");

        fieldTypeMap = new HashMap<>();
        // TODO Field Type Map Table
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
    public static void checkFieldType(List<String> dstFields, List<String> srcFields)
            throws FieldMapException {
        String dstField = dstFields.get(0);
        String srcField = srcFields.get(0);
        FieldTypeEnum dstFieldType = fieldTypeMap.get(dstField);
        FieldTypeEnum srcFieldType = fieldTypeMap.get(srcField);

        if (srcFieldType != dstFieldType) {
            throw new FieldMapException(srcField + "'s type is not adapted to " + dstField);
        }
    }

    public static void checkFieldName(List<String> dstFields, List<String> srcFields,
                                      List<String> writeFields, List<String> readFields)
            throws FieldMapException {
        for (String dstField: dstFields) {
            if (!writeFields.contains(dstField)) {
                throw new FieldMapException("DstField, " + dstField + " doesn't exist");
            }
        }

        for (String srcField: srcFields) {
            if (!readFields.contains(srcField)) {
                throw new FieldMapException("SrcField, " + srcField + " doesn't exist");
            }
        }
    }

    public static FieldMapper parseRule(String rule) {
        List<String> srcFields = new ArrayList<>();
        List<String> dstFields = new ArrayList<>();

        // TODO 规则语法检查
        // 匹配目标字段
        Pattern dstPattern = Pattern.compile("(?<=\\{).*?(?=\\}=)");
        Matcher dstMatcher = dstPattern.matcher(rule);
        while (dstMatcher.find()) {
            dstFields.add(dstMatcher.group(0));
        }

        rule = rule.replaceAll("\\{.*?\\}=", "");

        // 匹配源字段
        Pattern srcPattern = Pattern.compile("(?<=\\{).*?(?=\\})");
        Matcher srcMatcher = srcPattern.matcher(rule);
        while (srcMatcher.find()) {
            srcFields.add(srcMatcher.group(0));
        }

        rule = rule.replaceAll("\\{.*?\\}", "%s");

        FieldMapper fieldMapper = new FieldMapper();
        fieldMapper.setDstFields(dstFields);
        fieldMapper.setSrcFields(srcFields);
        fieldMapper.setRule(rule);
        return fieldMapper;
    }

}
