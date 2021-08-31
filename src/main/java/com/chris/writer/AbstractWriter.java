package com.chris.writer;

import com.chris.syncData.SyncData;

import java.sql.Connection;
import java.sql.Statement;

public abstract class AbstractWriter {

    public WriterTypeEnum writerType;
    private SyncData syncData;
    private String url;
    private String username;
    private String password;
    private String tableName;
    private Connection connection;
    private Statement statement;

    // 配置数据库信息
    public abstract void config(String fileName);
    // 初始化同步数据集
    public abstract void init(SyncData syncData);
    public abstract void connect();
    public abstract void write();
    public abstract void close();
}
