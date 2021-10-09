package pers.chris.dbSync.conf;

import pers.chris.dbSync.common.typeEnum.DBTypeEnum;
import pers.chris.dbSync.util.ConnectUtil;

public class DBConf {

    public DBTypeEnum dbType;
    public String hostname;
    public String port;
    public String user;
    public String password;
    public String dbName;
    public String tableName;

    public String getUrl() {
        return ConnectUtil.getUrl(dbType, hostname, port, dbName);
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
