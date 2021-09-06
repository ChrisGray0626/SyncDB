package com.chris.writer;

import com.chris.config.WriterConfig;
import com.chris.syncData.SyncData;
import com.chris.syncData.SyncDataEvent;
import com.chris.util.ConnectUtil;
import com.chris.util.FieldsNameUtil;
import common.DBType;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreSQLWriter extends AbstractWriter {
    private String[] fieldsName;
    private Connection connection;
    private Statement statement;
    private static final Logger logger = Logger.getLogger(PostgreSQLWriter.class);

    public PostgreSQLWriter() {
        writerType = WriterTypeEnum.POSTGRESQL;
    }

    @Override
    public void config(String fileName) {
    }

    @Override
    public void connect() {
        connection = ConnectUtil.connect(DBType.POSTGRESQL, connection, super.getWriterConfig().getUrl(), super.getWriterConfig().getUser(), super.getWriterConfig().getPassword());
    }

    @Override
    public void write() {
        // TODO 监听器解耦
        SyncData syncData = super.getSyncData();
        syncData.registerListener(new SyncData.SyncDataListener() {
            @Override
            public void doSet(SyncDataEvent event) {
                String[] rows = syncData.getRows();
                SyncData.EventTypeEnum curEventTypeEnum = syncData.getEventType();

                String SQL = null;
                if (curEventTypeEnum == SyncData.EventTypeEnum.INSERT) {
                    SQL = insertSQL(rows);
                }

                try {
                    statement = connection.createStatement();
                    statement.execute(SQL);
                    logger.debug(SQL);
                } catch (SQLException e) {
                    logger.error(e);
                }
            }
        });
    }

    private String insertSQL(String[] rows) {

        StringBuilder values = new StringBuilder();
            for (String value : rows) {
                values.append("'").append(value).append("',");
            }
            values.deleteCharAt(values.length() - 1);

            String SQL = "INSERT INTO " + super.getWriterConfig().getTableName() + " VALUES(" + values + ")";
        return SQL;
    }

    public void setFieldsName() {
        fieldsName = FieldsNameUtil.getFieldsName(connection, super.getWriterConfig().getTableName());
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public void setWriterConfig(WriterConfig writerConfig) {
        super.setWriterConfig(writerConfig);
    }
}
