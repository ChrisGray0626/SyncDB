package pers.chris.dbSync.fieldMapper;


import org.apache.log4j.Logger;
import pers.chris.dbSync.exception.FieldMapException;
import pers.chris.dbSync.util.FieldUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldMapManager {

    private final List<FieldMapper> fieldMappers;
    private List<String> rules;
    private final Logger logger = Logger.getLogger(FieldMapManager.class);

    public FieldMapManager(List<String> rules) {
        fieldMappers = new ArrayList<>();
        this.rules = rules;
    }

    public void config() {
        for (String rule : rules) {
            fieldMappers.add(FieldUtil.parseRule(rule));
        }
    }

    public void check(List<String> writeFields, List<String> readFields) {
        try {
            for (FieldMapper fieldMapper: fieldMappers) {
                List<String> dstFields = fieldMapper.getDstFields();
                List<String> srcFields = fieldMapper.getSrcFields();
                FieldUtil.checkFieldType(dstFields, srcFields);
                FieldUtil.checkFieldName(dstFields, srcFields, writeFields, readFields);
            }
        } catch (FieldMapException e) {
            e.printStackTrace();
            logger.error(e);
        }
    }

    public Map<String, String> run(Map<String, String> rows) {
        for (FieldMapper fieldMapper : fieldMappers) {
            fieldMapper.map(rows);
        }
        return rows;
    }

    public List<String> getRules() {
        return rules;
    }

    public void setRules(List<String> rules) {
        this.rules = rules;
    }
}
