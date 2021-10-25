package pers.chris.dbSync.reader;

import pers.chris.dbSync.common.typeEnum.EventTypeEnum;
import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.conf.JobConf;
import pers.chris.dbSync.conf.SyncConf;
import pers.chris.dbSync.syncData.DataEvent;
import pers.chris.dbSync.syncData.DataListener;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.*;
import org.apache.log4j.Logger;
import pers.chris.dbSync.valueFilter.ValueFilterManager;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class Reader extends BaseReader {

    private DBConf readerConf;
    private SyncConf syncConf;
    private ValueFilterManager valueFilterManager;
    private Connection connection;
    private DataListener dataListener;
    private static final Logger logger = Logger.getLogger(Reader.class);

    @Override
    public void connect() {
        connection = ConnectUtil.connect(getReaderConf().dbType, getReaderConf().getUrl(), getReaderConf().user, getReaderConf().password);
    }

    // 读取字段信息（名字、类型）
    @Override
    public void readField() {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getColumns(null, "%", getReaderConf().tableName, "%");
            setFields(FieldUtil.read(resultSet));

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void trigger(SyncData syncData) {
        dataListener.doSet(new DataEvent(syncData));
    }

    public void registerListener(DataListener dataListener) {
        this.dataListener = dataListener;
    }

    // 数据过滤在读取时进行
    @Override
    public void read() {
        String valueFilterSQL = valueFilterManager.getFilterSQL();

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + getReaderConf().tableName
                            + " where " + valueFilterSQL);

            while (resultSet.next()) {
                SyncData syncData = new SyncData();
                Map<String, String> data = new HashMap<>();

                // 根据字段名称获取对应数据
                for (String fieldName : getFieldNames()) {
                    data.put(fieldName, resultSet.getString(fieldName));
                }

                syncData.eventType = EventTypeEnum.INSERT;
                syncData.setData(data);
                trigger(syncData);
            }
        }
        catch (SQLException e) {
            logger.error(e);
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

    public DBConf getReaderConf() {
        return readerConf;
    }

    public void setReaderConf(DBConf readerConf) {
        this.readerConf = readerConf;
    }

    public SyncConf getSyncConf() {
        return syncConf;
    }

    public void setSyncConf(SyncConf syncConf) {
        this.syncConf = syncConf;
    }

    public void setValueFilterManager(ValueFilterManager valueFilterManager) {
        this.valueFilterManager = valueFilterManager;
    }

    public Connection getConnection() {
        return connection;
    }

}
