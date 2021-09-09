package pers.chris.dbSync.writer;

import pers.chris.dbSync.conf.DBConf;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.ConnectUtil;
import pers.chris.dbSync.util.FieldUtil;
import pers.chris.dbSync.util.GenerateSQLUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class Writer extends AbstractWriter {

    private DBConf writerConf;
    private Connection connection;
    private static final Logger logger = Logger.getLogger(Writer.class);

    @Override
    public void connect() {
        connection = ConnectUtil.connect(getWriterConfig().dbType, getWriterConfig().getUrl(), getWriterConfig().getUser(), getWriterConfig().getPassword());
        assert connection != null;
        setFields(FieldUtil.readFields(connection, getWriterConfig().getTableName()));
    }

    @Override
    public void write(SyncData syncData) {
        Map<String, String> rows = syncData.getRows();
        SyncData.EventTypeEnum curEventType = syncData.getEventType();

        String SQL = null;
        switch (curEventType) {
            case INSERT:
                SQL = GenerateSQLUtil.insertSQL(getWriterConfig().getTableName(), rows);
                break;
            default:
        }

        try {
            Statement statement = connection.createStatement();
            statement.execute(SQL);
            logger.debug(SQL);
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
