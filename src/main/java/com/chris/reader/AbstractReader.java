package com.chris.reader;

import com.chris.syncData.SyncData;

import java.sql.Connection;
import java.sql.Statement;

public abstract class AbstractReader {

    public ReaderTypeEnum readerType;
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
    public abstract void initSyncData(SyncData syncData);
    public abstract void connect();
    public abstract void read();
    public abstract void close();

    public String Enum2Str (ReaderTypeEnum readerType) {
        switch (readerType) {
            case MYSQL:
                return "MySQL";
            case POSTGRESQL:
                return "PostgreSQL";
            case SQLSERVER:
                return "SQLServer";
            default:
                break;
        }
        return "";
    }
}