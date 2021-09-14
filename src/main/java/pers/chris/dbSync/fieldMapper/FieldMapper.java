package pers.chris.dbSync.fieldMapper;

import org.apache.log4j.Logger;
import pers.chris.dbSync.exception.FieldMapException;
import pers.chris.dbSync.util.FieldUtil;

import java.util.List;
import java.util.Map;

public class FieldMapper {

    private List<String> srcFields; // 映射源字段
    private List<String> dstFields; // 映射目标字段
    private String rule; // 映射规则
    private final Logger logger = Logger.getLogger(FieldMapper.class);

    public void check() {
        try {
            FieldUtil.check(srcFields, dstFields);
        } catch (FieldMapException e) {
            logger.error(e);
        }
    }

    public Map<String, String> map(Map<String, String> rows) {
            if (srcFields.size() == 1) {
                rows = multi2One(rows);
            }
        return rows;
    }

    public Map<String, String> multi2One(Map<String, String> rows) {
        String dstField = dstFields.get(0);
        StringBuilder dstValue = new StringBuilder();

        for (String field: srcFields) {
            String value = rows.get(field);
            dstValue.append(value);
            rows.remove(field);
        }

        rows.put(dstField, dstValue.toString());
        return rows;
    }

    public List<String> getSrcFields() {
        return srcFields;
    }

    public void setSrcFields(List<String> srcFields) {
        this.srcFields = srcFields;
    }

    public List<String> getDstFields() {
        return dstFields;
    }

    public void setDstFields(List<String> dstFields) {
        this.dstFields = dstFields;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

}
