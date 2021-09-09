package pers.chris.dbSync.job;

import pers.chris.dbSync.conf.Conf;
import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.conf.SyncDataConf;
import pers.chris.dbSync.util.ConnectUtil;
import pers.chris.dbSync.common.DBTypeEnum;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DBSyncJobManager {

    List<DBSyncJob> dbSyncJobs;
    public DBTypeEnum dbType;
    private String hostname;
    private String dbName;
    private String port;
    private String user;
    private String password;
    private String tableName;
    private Connection connection;
    private final Logger logger = Logger.getLogger(Conf.class);

    public DBSyncJobManager() {
        dbSyncJobs = new ArrayList<>();
    }

    public void config (String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            logger.error(e);
        }

        dbType = DBTypeEnum.valueOf(properties.getProperty("conf.dbType").toUpperCase());
        hostname = properties.getProperty("conf.hostname");
        dbName = properties.getProperty("conf.dbName");
        port = properties.getProperty("conf.port");
        user = properties.getProperty("conf.user");
        password = properties.getProperty("conf.password");
        tableName = properties.getProperty("conf.tableName");
    }

    private void connect() {
        String url = ConnectUtil.getUrl(dbType, hostname, port, dbName);
        connection = ConnectUtil.connect(dbType, url, user, password);
    }

    public void configJob() {
        try {
            connect();

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from " + tableName);

            while (resultSet.next()) {
                DBSyncJob dbSyncJob = new DBSyncJob();
                SyncDataConf syncDataConf = new SyncDataConf();
                DBConf writerConf = new DBConf();
                DBConf readerConf = new DBConf();
                String jobId = resultSet.getString("task_id");
                syncDataConf.setInterval(Integer.parseInt(resultSet.getString("sync_interval")));
                syncDataConf.setTimeField(resultSet.getString("sync_time_field_name"));
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

                dbSyncJob.setJobId(jobId);
                dbSyncJob.setSyncDataConf(syncDataConf);
                dbSyncJob.setWriterConf(writerConf);
                dbSyncJob.setReaderConf(readerConf);

                dbSyncJobs.add(dbSyncJob);
            }

            close();
        } catch (SQLException e) {
            logger.error(e);
        }

        close();
    }

    public void run() {
        for (DBSyncJob dbSyncJob: dbSyncJobs) {
            Thread jobThead = new Thread(dbSyncJob);
            jobThead.start();
        }
    }

    private void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e);
        }
    }
}
