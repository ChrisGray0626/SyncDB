package com.chris.util;

import common.DBType;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class ConnectUtil {

    private static final Map<DBType, String> map = new HashMap<>();
    private static final Logger logger = Logger.getLogger(ConnectUtil.class);

    static {
        map.put(DBType.MYSQL, "com.mysql.cj.jdbc.Driver");
        map.put(DBType.POSTGRESQL, "org.postgresql.Driver");
        map.put(DBType.SQLSERVER, "com.microsoft.sqlserver.jdbc.SQLServerDriver");
    }

    public static Connection connect(DBType dbType, Connection connection, String url, String user, String password) {
        String driverName = map.get(dbType);
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException | SQLException e) {
            logger.error(e);
        }
        return connection;
    }
}
