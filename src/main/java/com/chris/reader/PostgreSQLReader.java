package com.chris.reader;

import com.chris.syncData.SyncData;
import com.chris.util.ConnectUtil;
import com.chris.util.FieldsNameUtil;
import com.chris.util.ParseUtil;
import com.chris.writer.PostgreSQLWriter;
import common.DBType;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.concurrent.TimeUnit;

public class PostgreSQLReader extends AbstractReader {
    private String[] fieldsName;
    private Connection connection;
    private Statement statement;
    private String logicalReplicationSlotName;
    private static final Logger logger = Logger.getLogger(PostgreSQLWriter.class);

    public PostgreSQLReader() {
        readerType = ReaderTypeEnum.POSTGRESQL;
        // 默认逻辑复制插槽名称
        logicalReplicationSlotName = "test_slot";
    }

    @Override
    public void config(String fileName) {
    }

    @Override
    public void connect() {
        connection = ConnectUtil.connect(DBType.POSTGRESQL, connection, super.getReaderConfig().getUrl(), super.getReaderConfig().getUser(), super.getReaderConfig().getPassword());
    }

    public void read(Integer interval) {
        while (true) {
            readLogicalSlot(super.getSyncData());
            try {
                TimeUnit.MINUTES.sleep(interval);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    // 默认轮询五分钟
    public void read() {
        read(5);
    }

    // 读取逻辑复制插槽
    private void readLogicalSlot(SyncData syncData) {
        try {
            statement = connection.createStatement();
            // 查看并消费插槽数据
            ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_logical_slot_get_changes('" + logicalReplicationSlotName + "', NULL, NULL)");
            ParseUtil.parsePGLogicalSlot(resultSet, syncData, super.getReaderConfig().getTableName());
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public void setFieldsName() {
        fieldsName = FieldsNameUtil.getFieldsName(connection, super.getReaderConfig().getTableName());
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // 自定义逻辑复制插槽名称
    public void setLogicalReplicationSlotName(String logicalReplicationSlotName) {
        this.logicalReplicationSlotName = logicalReplicationSlotName;
    }
}
