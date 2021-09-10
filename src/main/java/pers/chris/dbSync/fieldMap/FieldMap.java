package pers.chris.dbSync.fieldMap;

import java.util.List;

public class FieldMap {

    private List<String> srcFields; // 映射源字段
    private List<String> dstFields; // 映射目标字段
    private String rule; // 映射规则

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
