package pers.chris.dbSync.reader;

import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.ConnectUtil;
import pers.chris.dbSync.util.FieldUtil;
import pers.chris.dbSync.util.ResultSetParseUtil;
import pers.chris.dbSync.util.TimeUtil;
import org.apache.log4j.Logger;
import pers.chris.dbSync.valueFilter.ValueFilterManager;

import java.sql.*;
import java.util.concurrent.TimeUnit;

public class Reader extends AbstractReader{

    private DBConf readerConf;
    private ValueFilterManager valueFilterManager;
    private Connection connection;
    private static final Logger logger = Logger.getLogger(Reader.class);

    @Override
    public void connect() {
        connection = ConnectUtil.connect(getReaderConfig().dbType, getReaderConfig().getUrl(), getReaderConfig().getUser(), getReaderConfig().getPassword());
        assert connection != null;
    }

    // 读取字段信息（名字、类型）
    @Override
    public void readField() {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getColumns(null, "%", getReaderConfig().getTableName(), "%");
            setFields(FieldUtil.read(resultSet));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void read(SyncData syncData, Integer interval) {
        String timeFieldName = syncData.getSyncDataConfig().getTimeField();
        while (true) {
            try {
                String time = TimeUtil.intervalTime(interval); // 当前时间
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(
                        "select * from " + getReaderConfig().getTableName()
                                + " where " + timeFieldName + ">=" + time
                                + valueFilterManager.filterSQL());
                ResultSetParseUtil.parseGeneralSQL(resultSet, syncData, getFieldNames());
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

    public ValueFilterManager getValueFilterManager() {
        return valueFilterManager;
    }

    public void setValueFilterManager(ValueFilterManager valueFilterManager) {
        this.valueFilterManager = valueFilterManager;
    }

    public Connection getConnection() {
        return connection;
    }

}
