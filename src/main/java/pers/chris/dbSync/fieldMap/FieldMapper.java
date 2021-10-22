package pers.chris.dbSync.fieldMap;

import org.apache.log4j.Logger;
import pers.chris.dbSync.common.module.Module;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Map;

public class FieldMapper extends Module {

    private final List<String> dstFieldNames; // 目标字段
    private final List<String> srcFieldNames; // 源字段
    private final Logger logger = Logger.getLogger(FieldMapper.class);

    public FieldMapper(String rule) {
        super(rule);
        dstFieldNames = new ArrayList<>();
        srcFieldNames = new ArrayList<>();
    }

    public void run(Map<String, String> data) {
        // 当前仅支持映射到唯一目标字段
        String dstField = dstFieldNames.get(0);
        List<String> srcValues = new ArrayList<>();
        for (String srcField: srcFieldNames) {
            srcValues.add(data.get(srcField));
        }
        // 格式化目标值
        Formatter formatter = new Formatter();
        formatter.format(super.getRule(), srcValues.toArray());
        String dstValue = formatter.toString();
        // 移除源字段
        for (String srcField: srcFieldNames) {
            data.remove(srcField);
        }
        // 新增目标字段
        data.put(dstField, dstValue);
    }

    public List<String> getDstFieldNames() {
        return dstFieldNames;
    }

    public List<String> getSrcFieldNames() {
        return srcFieldNames;
    }

}
