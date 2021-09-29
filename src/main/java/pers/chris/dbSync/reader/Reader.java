package pers.chris.dbSync.reader;

import pers.chris.dbSync.common.typeEnum.EventTypeEnum;
import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.conf.JobConf;
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
    private JobConf jobConf;
    private ValueFilterManager valueFilterManager;
    private Connection connection;
    private DataListener dataListener;
    private static final Logger logger = Logger.getLogger(Reader.class);

    @Override
    public void connect() {
        connection = ConnectUtil.connect(getReaderConf().dbType, getReaderConf().getUrl(), getReaderConf().getUser(), getReaderConf().getPassword());
    }

    // 读取字段信息（名字、类型）
    @Override
    public void readField() {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getColumns(null, "%", getReaderConf().getTableName(), "%");
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

    // 数据过滤位置
    @Override
    public void read() {
        // TODO 时间字段过滤
        String timeFieldName = jobConf.getTimeField(); //时间字段
        String delayTime = TimeUtil.intervalTime(jobConf.getInterval());
        String timeFilterSQL = SQLGenerateUtil.timeFilterSQL(timeFieldName, delayTime); // 时间过滤语句
        String valueFilterSQL = valueFilterManager.filterSQL() + timeFilterSQL;

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from " + getReaderConf().getTableName()
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

    public JobConf getJobConf() {
        return jobConf;
    }

    public void setJobConf(JobConf jobConf) {
        this.jobConf = jobConf;
    }

    public void setValueFilterManager(ValueFilterManager valueFilterManager) {
        this.valueFilterManager = valueFilterManager;
    }

    public Connection getConnection() {
        return connection;
    }

}
