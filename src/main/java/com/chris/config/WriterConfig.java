package com.chris.config;


import com.chris.writer.WriterTypeEnum;

public class WriterConfig {

    public WriterTypeEnum writerType;
    private String hostname;
    private String port;
    private String user;
    private String password;
    private String databaseName;
    private String tableName;

    public String getUrl() {
        return "jdbc:" + writerType.toString().toLowerCase() + "://" + hostname + ":" + port + "/" + databaseName;
    }

    public WriterTypeEnum getWriterType() {
        return writerType;
    }

    public void setWriterType(WriterTypeEnum writerType) {
        this.writerType = writerType;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
