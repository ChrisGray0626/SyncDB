package com.chris.writer;

import com.chris.configuration.WriterConfiguration;
import com.chris.syncData.SyncData;
import com.chris.util.ConnectUtil;
import com.chris.util.FieldUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class Writer extends AbstractWriter {
    private Connection connection;
    private static final Logger logger = Logger.getLogger(Writer.class);

    @Override
    public void connect() {
        connection = ConnectUtil.connect(getWriterConfig().dbType, getWriterConfig().getUrl(), getWriterConfig().getUser(), getWriterConfig().getPassword());
        setFieldNames(FieldUtil.readFieldName(connection, getWriterConfig().getTableName()));
    }

    @Override
    public void write(SyncData syncData) {
        List<String> rows = syncData.getRows();
        SyncData.EventTypeEnum curEventType = syncData.getEventType();

        String SQL = null;
        switch (curEventType) {
            case INSERT:
                SQL = insertSQL(rows);
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

    private String insertSQL(List<String> rows) {

        StringBuilder values = new StringBuilder();
            for (String value : rows) {
                values.append("'").append(value).append("',");
            }
            values.deleteCharAt(values.length() - 1);

            String SQL = "INSERT INTO " + super.getWriterConfig().getTableName() + " VALUES(" + values + ")";
        return SQL;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public void setWriterConfig(WriterConfiguration writerConfiguration) {
        super.setWriterConfig(writerConfiguration);
    }
}
