package pers.chris.dbSync.reader;

import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.ConnectUtil;
import pers.chris.dbSync.util.FieldUtil;
import pers.chris.dbSync.util.ResultSetUtil;
import pers.chris.dbSync.util.TimeUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class Reader extends AbstractReader{

    private DBConf readerConf;
    private Connection connection;
    private static final Logger logger = Logger.getLogger(Reader.class);

    @Override
    public void connect() {
        connection = ConnectUtil.connect(getReaderConfig().dbType, getReaderConfig().getUrl(), getReaderConfig().getUser(), getReaderConfig().getPassword());
        assert connection != null;
        // 获取字段
        setFields(FieldUtil.readFields(connection, getReaderConfig().getTableName()));
    }

    // 默认read方式，Pull
    @Override
    public void read(SyncData syncData, Integer interval) {
        String time = TimeUtil.intervalTime(interval);
        String timeFieldName = syncData.getSyncDataConfig().getTimeField();
        while (true) {
            try {
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(
                        "select * from " + getReaderConfig().getTableName()
                                + " where " + timeFieldName + " >= " + time);
                ResultSetUtil.parseGeneralSQL(resultSet, syncData, getFields());
            } catch (SQLException e) {
                logger.error(e);
            }

            try {
                TimeUnit.MINUTES.sleep(interval);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public DBConf getReaderConfig() {
        return readerConf;
    }

    public void setReaderConfig(DBConf readerConf) {
        this.readerConf = readerConf;
    }

    public Connection getConnection() {
        return connection;
    }

}
