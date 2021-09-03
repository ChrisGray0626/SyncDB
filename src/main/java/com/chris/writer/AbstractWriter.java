package com.chris.writer;

import com.chris.syncData.SyncData;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

public abstract class AbstractWriter {

    public WriterTypeEnum writerType;
    private SyncData syncData;
    private String url;
    private String username;
    private String password;
    private String tableName;
    private String[] fieldsName;
    private Connection connection;
    private Statement statement;

    // 配置数据库信息
    public abstract void config(String fileName);
    public abstract void setSyncData(SyncData syncData);
    public abstract void connect();
    public abstract void write();
    public abstract void setFieldsName();
    public abstract void close();
}
