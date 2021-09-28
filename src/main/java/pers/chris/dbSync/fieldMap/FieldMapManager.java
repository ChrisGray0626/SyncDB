package pers.chris.dbSync.fieldMap;


import org.apache.log4j.Logger;
import pers.chris.dbSync.exception.FieldMapException;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.FieldUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldMapManager {

    private final List<FieldMapper> fieldMappers;
    private final Logger logger = Logger.getLogger(FieldMapManager.class);

    public FieldMapManager(List<String> rules) {
        fieldMappers = new ArrayList<>();
        load(rules.toArray(new String[0]));
    }

    public void add(String rule) {
        load(rule);
    }

    public void addAll(List<String> rules) {
        load(rules.toArray(new String[0]));
    }

    private void load(String... rules) {
        for (String rule: rules) {
            ensureRule(rule);
            fieldMappers.add(FieldUtil.parseRule(rule));
        }
    }

    // 规则检查
    public void ensureRule(String... rules) {
        try {
            for (String rule: rules) {
                FieldUtil.checkRuleSyntax(rule);
            }
        } catch (FieldMapException e) {
            logger.error(e);
        }
    }

    // 字段检查
    public void ensureField(Map<String, String> writeFields, Map<String, String> readFields) {
        List<String> writeFieldNames = new ArrayList<>(writeFields.keySet());
        List<String> readFieldNames = new ArrayList<>(readFields.keySet());

        try {
            for (FieldMapper fieldMapper: fieldMappers) {
                List<String> dstFieldNames = fieldMapper.getDstFieldNames();
                List<String> srcFieldNames = fieldMapper.getSrcFieldNames();

                FieldUtil.checkFieldName(dstFieldNames, srcFieldNames, writeFieldNames, readFieldNames);
                FieldUtil.checkFieldType(dstFieldNames, srcFieldNames, writeFields, readFields);
            }
        } catch (FieldMapException e) {
            logger.error(e);
        }
    }

    @Deprecated
    public Map<String, String> run(Map<String, String> rows) {
        for (FieldMapper fieldMapper : fieldMappers) {
            fieldMapper.map(rows);
        }
        return rows;
    }

    public void run(SyncData syncData) {
        ensureField(syncData.getWriteFields(), syncData.getReadFields());

        Map<String, String> data = syncData.getData();

        for (FieldMapper fieldMapper : fieldMappers) {
            fieldMapper.map(data);
        }

        syncData.setData(data);
    }

}
