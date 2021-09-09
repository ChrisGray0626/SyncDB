package pers.chris.dbSync.mapper;

import java.util.List;

public class FieldMapper {

    private List<String> srcFields; // 映射源字段
    private List<String> dstFields; // 映射目标字段
    private String mapRule; // 映射规则

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

    public String getMapRule() {
        return mapRule;
    }

    public void setMapRule(String mapRule) {
        this.mapRule = mapRule;
    }
}
