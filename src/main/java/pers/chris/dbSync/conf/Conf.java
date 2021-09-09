package pers.chris.dbSync.conf;

import pers.chris.dbSync.util.ConnectUtil;
import pers.chris.dbSync.common.DBTypeEnum;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class Conf {

    private String confId;
    private final DBConf writerConf;
    private final DBConf readerConf;
    private final SyncDataConf syncDataConf;
    public DBTypeEnum dbType;
    private String hostname;
    private String dbName;
    private String port;
    private String user;
    private String password;
    private String tableName;
    private Connection connection;
    private final Logger logger = Logger.getLogger(Conf.class);

    public Conf() {
        writerConf = new DBConf();
        readerConf = new DBConf();
        syncDataConf = new SyncDataConf();
    }

    public void config (String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            logger.error(e);
        }

        dbType = DBTypeEnum.valueOf(properties.getProperty("configuration.dbType").toUpperCase());
        hostname = properties.getProperty("configuration.hostname");
        dbName = properties.getProperty("configuration.dbName");
        port = properties.getProperty("configuration.port");
        user = properties.getProperty("configuration.user");
        password = properties.getProperty("configuration.password");
        tableName = properties.getProperty("configuration.tableName");
    }

    private void connect() {
        connection = ConnectUtil.connect(dbType, getUrl(), user, password);
    }

    public void configTask() {
        try {
            connect();

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from " + tableName + " where config_id=" + confId);

            resultSet.next(); // 光标默认位于第一行之前，需要移动至下一行
            writerConf.setDBType(DBTypeEnum.valueOf(resultSet.getString("writer_db_type")));
            writerConf.setHostname(resultSet.getString("writer_hostname"));
            writerConf.setPort(resultSet.getString("writer_port"));
            writerConf.setDBName(resultSet.getString("writer_db_name"));
            writerConf.setUser(resultSet.getString("writer_user"));
            writerConf.setPassword(resultSet.getString("writer_password"));
            writerConf.setTableName(resultSet.getString("writer_table_name"));
            readerConf.setDBType(DBTypeEnum.valueOf(resultSet.getString("reader_db_type")));
            readerConf.setHostname(resultSet.getString("reader_hostname"));
            readerConf.setPort(resultSet.getString("reader_port"));
            readerConf.setDBName(resultSet.getString("reader_db_name"));
            readerConf.setUser(resultSet.getString("reader_user"));
            readerConf.setPassword(resultSet.getString("reader_password"));
            readerConf.setTableName(resultSet.getString("reader_table_name"));
            syncDataConf.setInterval(Integer.parseInt(resultSet.getString("sync_interval")));
            syncDataConf.setTimeField(resultSet.getString("sync_time_field_name"));
        } catch (SQLException e) {
            logger.error(e);
        }

        close();
    }

    private void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public String getUrl() {
        return "jdbc:" + dbType.toString().toLowerCase() + "://" + hostname + ":" + port + "/" + dbName;
    }

    public void setConfId(String confId) {
        this.confId = confId;
    }

    public DBConf getWriterConf() {
        return writerConf;
    }

    public DBConf getReaderConf() {
        return readerConf;
    }

    public SyncDataConf getSyncDataConf() {
        return syncDataConf;
    }
}
