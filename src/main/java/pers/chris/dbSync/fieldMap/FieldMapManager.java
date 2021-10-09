package pers.chris.dbSync.fieldMap;


import org.apache.log4j.Logger;
import pers.chris.dbSync.exception.FieldMapException;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.FieldUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldMapManager {

    private final Map<String, FieldMapper> fieldMappers;
    private Map<String, String> readFields;
    private Map<String, String> writeFields;
    private final Logger logger = Logger.getLogger(FieldMapManager.class);

    public FieldMapManager(Map<String, FieldMapper> fieldMappers) {
        this.fieldMappers = fieldMappers;
    }

    public void load() {
        for (FieldMapper fieldMapper: fieldMappers.values()) {
            checkRule(fieldMapper.getRule());
            parseRule(fieldMapper);
            checkField(fieldMapper);
        }
    }

    public void checkRule(String rule) {
        try {
            Pattern pattern = Pattern.compile("\\{.*?\\}=(([\\s\\S]*)\\{.*?\\})+");
            Matcher matcher = pattern.matcher(rule);

            if (!matcher.matches()) {
                throw new FieldMapException("Rule Syntax Error: '" + rule + "' exists syntax error");
            }
        } catch (FieldMapException e) {
            logger.error(e);
        }
    }

    public void parseRule(FieldMapper fieldMapper) {
        String rule = fieldMapper.getRule();

        // 匹配目标字段"{DstFieldName}"
        Pattern dstPattern = Pattern.compile("(?<=\\{).*?(?=\\}=)");
        Matcher dstMatcher = dstPattern.matcher(rule);
        while (dstMatcher.find()) {
            fieldMapper.getDstFieldNames().add(dstMatcher.group(0));
        }

        // 去除目标字段内容"{DstFieldName}="
        rule = rule.replaceAll("\\{.*?\\}=", "");

        // 匹配源字段"{SrcFieldName}"
        Pattern srcPattern = Pattern.compile("(?<=\\{).*?(?=\\})");
        Matcher srcMatcher = srcPattern.matcher(rule);
        while (srcMatcher.find()) {
            fieldMapper.getSrcFieldNames().add(srcMatcher.group(0));
        }

        // 替换源字段内容"{SrcFieldName}"为"%s"
        rule = rule.replaceAll("\\{.*?\\}", "%s");

        fieldMapper.setRule(rule);
    }

    // 字段检查
    public void checkField(FieldMapper fieldMapper) {
        try {
            List<String> dstFieldNames = fieldMapper.getDstFieldNames();
            List<String> srcFieldNames = fieldMapper.getSrcFieldNames();

            FieldUtil.checkFieldName(dstFieldNames, srcFieldNames,
                    new ArrayList<>(writeFields.keySet()), new ArrayList<>(readFields.keySet()));
            FieldUtil.checkFieldType(dstFieldNames, srcFieldNames, writeFields, readFields);
        } catch (FieldMapException e) {
            logger.error(e);
        }
    }

    public void run(SyncData syncData) {
        Map<String, String> data = syncData.getData();

        for (FieldMapper fieldMapper : fieldMappers.values()) {
            fieldMapper.map(data);
        }

        syncData.setData(data);
    }

    public void setReadFields(Map<String, String> readFields) {
        this.readFields = readFields;
    }

    public void setWriteFields(Map<String, String> writeFields) {
        this.writeFields = writeFields;
    }

}
