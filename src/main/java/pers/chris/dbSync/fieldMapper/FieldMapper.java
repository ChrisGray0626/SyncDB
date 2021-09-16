package pers.chris.dbSync.fieldMapper;

import org.apache.log4j.Logger;
import pers.chris.dbSync.common.FieldTypeEnum;
import pers.chris.dbSync.exception.FieldMapException;
import pers.chris.dbSync.util.FieldUtil;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Map;

public class FieldMapper {

    private String rule; // 映射规则
    private List<String> dstFields; // 目标字段
    private List<String> srcFields; // 源字段
    private final Logger logger = Logger.getLogger(FieldMapper.class);

    public Map<String, String> map(Map<String, String> rows) {
            if (dstFields.size() == 1) {
                rows = multi2One(rows);
            }
        return rows;
    }

    // 多对一关系映射
    public Map<String, String> multi2One(Map<String, String> rows) {
        String dstField = dstFields.get(0);
        List<String> srcValues = new ArrayList<>();

        for (String srcField: srcFields) {
            srcValues.add(rows.get(srcField));
        }

        // 格式化目标值
        Formatter formatter = new Formatter();
        formatter.format(rule, srcValues.toArray());
        String dstValue = formatter.toString();

        // 移除源字段
        for (String srcField: srcFields) {
            rows.remove(srcField);
        }

        // 新增目标字段
        rows.put(dstField, dstValue);
        return rows;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public List<String> getDstFields() {
        return dstFields;
    }

    public void setDstFields(List<String> dstFields) {
        this.dstFields = dstFields;
    }

    public List<String> getSrcFields() {
        return srcFields;
    }

    public void setSrcFields(List<String> srcFields) {
        this.srcFields = srcFields;
    }

}
