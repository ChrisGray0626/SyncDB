package pers.chris.dbSync.common;


import java.util.ArrayList;
import java.util.List;

public abstract class Task {

    public DBTypeEnum dbType;
    private List<String> fields = new ArrayList<>();

    public abstract void connect();
    public abstract void close();

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }
}
