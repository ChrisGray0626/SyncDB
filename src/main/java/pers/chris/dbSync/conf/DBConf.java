package pers.chris.dbSync.conf;

import pers.chris.dbSync.common.typeEnum.DBTypeEnum;
import pers.chris.dbSync.util.ConnectUtil;

public class DBConf {

    public DBTypeEnum dbType;
    private String hostname;
    private String port;
    private String user;
    private String password;
    private String dbName;
    private String tableName;

    public String getUrl() {
        return ConnectUtil.getUrl(dbType, hostname, port, dbName);
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

    public String getDBName() {
        return dbName;
    }

    public void setDBName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "DBConf{" +
                "dbType=" + dbType +
                ", hostname='" + hostname + '\'' +
                ", port='" + port + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
