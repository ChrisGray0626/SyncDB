package pers.chris.dbSync.reader;

import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.*;
import org.apache.log4j.Logger;
import pers.chris.dbSync.valueFilter.ValueFilterManager;

import java.sql.*;
import java.util.concurrent.TimeUnit;

public class Reader extends BaseReader {

    private DBConf readerConf;
    private ValueFilterManager valueFilterManager;
    private Connection connection;
    private static final Logger logger = Logger.getLogger(Reader.class);

    @Override
    public void connect() {
        connection = ConnectUtil.connect(getReaderConfig().dbType, getReaderConfig().getUrl(), getReaderConfig().getUser(), getReaderConfig().getPassword());
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

    // 数据过滤位置
    @Override
    public void read(SyncData syncData, Integer interval) {
        String timeFieldName = syncData.getSyncDataConfig().getTimeField(); //时间字段
        while (true) {
            try {
                // 时间过滤
                String curTime = TimeUtil.intervalTime(interval); // 当前时间
                String timeFilterSQL = SQLGenerateUtil.timeFilterSQL(timeFieldName, curTime); // 时间过滤语句
                String valueFilterSQL = valueFilterManager.filterSQL() + timeFilterSQL;

                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(
                        "select * from " + getReaderConfig().getTableName()
                                + " where " + valueFilterSQL);
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
