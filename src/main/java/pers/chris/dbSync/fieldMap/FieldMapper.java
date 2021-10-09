package pers.chris.dbSync.fieldMap;

import org.apache.log4j.Logger;
import pers.chris.dbSync.exception.FieldMapException;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldMapper {

    private String rule; // 映射规则
    private final List<String> dstFieldNames; // 目标字段
    private final List<String> srcFieldNames; // 源字段
    private final Logger logger = Logger.getLogger(FieldMapper.class);

    public FieldMapper(String rule) {
        dstFieldNames = new ArrayList<>();
        srcFieldNames = new ArrayList<>();
        this.rule = rule;
    }

    public Map<String, String> map(Map<String, String> rows) {
        // 当前仅支持映射到唯一目标字段
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

    public List<String> getSrcFieldNames() {
        return srcFieldNames;
    }

}
