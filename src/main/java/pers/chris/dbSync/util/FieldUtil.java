package pers.chris.dbSync.util;

import org.apache.log4j.Logger;
import pers.chris.dbSync.common.FieldTypeEnum;
import pers.chris.dbSync.exception.FieldMapException;
import pers.chris.dbSync.fieldMap.FieldMapper;

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
        fieldTypeMap = new HashMap<>();

        pointlessFields.add("USER");
        pointlessFields.add("CURRENT_CONNECTIONS");
        pointlessFields.add("TOTAL_CONNECTIONS");

        fieldTypeMap.put("id", FieldTypeEnum.INT);
        fieldTypeMap.put("serial", FieldTypeEnum.INT);
        fieldTypeMap.put("int", FieldTypeEnum.INT);
        fieldTypeMap.put("int2", FieldTypeEnum.INT);
        fieldTypeMap.put("int4", FieldTypeEnum.INT);
        fieldTypeMap.put("int8", FieldTypeEnum.INT);
        fieldTypeMap.put("integer", FieldTypeEnum.INT);
        fieldTypeMap.put("tinyint", FieldTypeEnum.INT);
        fieldTypeMap.put("smallint", FieldTypeEnum.INT);
        fieldTypeMap.put("mediumint", FieldTypeEnum.INT);
        fieldTypeMap.put("bigint", FieldTypeEnum.INT);
        fieldTypeMap.put("float4", FieldTypeEnum.FLOAT);
        fieldTypeMap.put("float8", FieldTypeEnum.FLOAT);
        fieldTypeMap.put("double", FieldTypeEnum.FLOAT);
        fieldTypeMap.put("decimal", FieldTypeEnum.FLOAT);
        fieldTypeMap.put("char", FieldTypeEnum.STRING);
        fieldTypeMap.put("character", FieldTypeEnum.STRING);
        fieldTypeMap.put("varchar", FieldTypeEnum.STRING);
        fieldTypeMap.put("text", FieldTypeEnum.STRING);
        fieldTypeMap.put("date", FieldTypeEnum.TIME);
        fieldTypeMap.put("time", FieldTypeEnum.TIME);
        fieldTypeMap.put("datetime", FieldTypeEnum.TIME);
        fieldTypeMap.put("timestamp", FieldTypeEnum.TIME);
        fieldTypeMap.put("boolean", FieldTypeEnum.BOOLEAN);
    }

    public static Map<String, String> read(ResultSet resultSet) {
        // LinkedHashMap保证插入顺序
        Map<String, String> fields = new LinkedHashMap<>();

        try {
            while (resultSet.next()) {
                String field = resultSet.getString("COLUMN_NAME");
                String type = resultSet.getString("TYPE_NAME").toLowerCase();
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
        Map<String, String> data = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            data.put(fields.get(i), values.get(i));
        }
        return data;
    }

    // 字段名检查
    public static void checkFieldName(List<String> dstFields, List<String> srcFields,
                                      List<String> writeFields, List<String> readFields)
            throws FieldMapException {
        for (String dstField: dstFields) {
            if (!writeFields.contains(dstField)) {
                throw new FieldMapException("DstField Error: " + dstField + " doesn't exist");
            }
        }

        for (String srcField: srcFields) {
            if (!readFields.contains(srcField)) {
                throw new FieldMapException("SrcField Error: " + srcField + " doesn't exist");
            }
        }
    }

    // 字段类型检查
    public static void checkFieldType(List<String> dstFieldNames, List<String> srcFieldNames, Map<String, String> writeFields, Map<String, String> readFields)
            throws FieldMapException {
        String dstFieldName = dstFieldNames.get(0);
        String srcFieldName = srcFieldNames.get(0);
        FieldTypeEnum dstFieldType = fieldTypeMap.get(writeFields.get(dstFieldName));
        FieldTypeEnum srcFieldType = fieldTypeMap.get(readFields.get(srcFieldName));

        if (srcFieldType != dstFieldType) {
            throw new FieldMapException("Type Error: " + srcFieldName + "'s type is not adapted to " + dstFieldName);
        }
    }

    // 规则语法检查
    public static void checkRuleSyntax(String rule)
            throws FieldMapException {
        Pattern pattern = Pattern.compile("\\{.*?\\}=(([\\s\\S]*)\\{.*?\\})+");
        Matcher matcher = pattern.matcher(rule);

        if (!matcher.matches()) {
            throw new FieldMapException("Rule Syntax Error: '" + rule + "' exists syntax error");
        }
    }

    public static FieldMapper parseRule(String rule) {
        List<String> srcFields = new ArrayList<>();
        List<String> dstFields = new ArrayList<>();

        // 匹配目标字段
        Pattern dstPattern = Pattern.compile("(?<=\\{).*?(?=\\}=)");
        Matcher dstMatcher = dstPattern.matcher(rule);
        while (dstMatcher.find()) {
            dstFields.add(dstMatcher.group(0));
        }

        // 去除目标字段内容
        rule = rule.replaceAll("\\{.*?\\}=", "");

        // 匹配源字段
        Pattern srcPattern = Pattern.compile("(?<=\\{).*?(?=\\})");
        Matcher srcMatcher = srcPattern.matcher(rule);
        while (srcMatcher.find()) {
            srcFields.add(srcMatcher.group(0));
        }

        // 替换源字段内容
        rule = rule.replaceAll("\\{.*?\\}", "%s");

        FieldMapper fieldMapper = new FieldMapper();
        fieldMapper.setDstFieldNames(dstFields);
        fieldMapper.setSrcFieldNames(srcFields);
        fieldMapper.setRule(rule);
        return fieldMapper;
    }

}
