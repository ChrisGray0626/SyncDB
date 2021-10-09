package pers.chris.dbSync.writer;

import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.common.typeEnum.EventTypeEnum;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.ConnectUtil;
import pers.chris.dbSync.util.FieldUtil;
import pers.chris.dbSync.util.SQLGenerateUtil;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Map;

public class Writer extends BaseWriter {

    private DBConf writerConf;
    private Connection connection;
    private static final Logger logger = Logger.getLogger(Writer.class);

    @Override
    public void connect() {
        connection = ConnectUtil.connect(getWriterConfig().dbType, getWriterConfig().getUrl(), getWriterConfig().user, getWriterConfig().password);
    }

    @Override
    public void readField() {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getColumns(null, "%", getWriterConfig().tableName, "%");
            setFields(FieldUtil.read(resultSet));
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void write(SyncData syncData) {
        Map<String, String> data = syncData.getData();

        String SQL = SQLGenerateUtil.insertSQL(getWriterConfig().tableName, data);

        try {
            Statement statement = connection.createStatement();
            logger.debug(SQL);
            statement.execute(SQL);
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public DBConf getWriterConfig() {
        return writerConf;
    }

    public void setWriterConfig(DBConf writerConf) {
        this.writerConf = writerConf;
    }
}
