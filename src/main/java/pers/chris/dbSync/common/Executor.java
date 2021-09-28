package pers.chris.dbSync.common;


import pers.chris.dbSync.common.typeEnum.DBTypeEnum;

import java.util.*;

public abstract class Executor {

    public DBTypeEnum dbType;
    private Map<String, String> fields;

    public abstract void connect();
    public abstract void readField();
    public abstract void close();

    public List<String> getFieldNames() {
        return new ArrayList<>(fields.keySet());
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }

}
