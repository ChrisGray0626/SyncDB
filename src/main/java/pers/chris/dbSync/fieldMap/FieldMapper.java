package pers.chris.dbSync.fieldMap;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Map;

public class FieldMapper {

    private String rule; // 映射规则
    private List<String> dstFieldNames; // 目标字段
    private List<String> srcFieldNames; // 源字段
    private final Logger logger = Logger.getLogger(FieldMapper.class);

    public Map<String, String> map(Map<String, String> rows) {
            if (dstFieldNames.size() == 1) {
                rows = multi2One(rows);
            }
        return rows;
    }

    // 多对一关系映射
    public Map<String, String> multi2One(Map<String, String> rows) {
        String dstField = dstFieldNames.get(0);
        List<String> srcValues = new ArrayList<>();
        for (String srcField: srcFieldNames) {
            srcValues.add(rows.get(srcField));
        }

        // 格式化目标值
        Formatter formatter = new Formatter();
        formatter.format(rule, srcValues.toArray());
        String dstValue = formatter.toString();

        // 移除源字段
        for (String srcField: srcFieldNames) {
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

    public List<String> getDstFieldNames() {
        return dstFieldNames;
    }

    public void setDstFieldNames(List<String> dstFieldNames) {
        this.dstFieldNames = dstFieldNames;
    }

    public List<String> getSrcFieldNames() {
        return srcFieldNames;
    }

    public void setSrcFieldNames(List<String> srcFieldNames) {
        this.srcFieldNames = srcFieldNames;
    }

}
