package com.chris.reader;

import com.chris.syncData.SyncData;
import com.chris.util.ConnectUtil;
import com.chris.util.ParseUtil;
import com.chris.writer.Writer;
import common.DBTypeEnum;
import com.chris.util.FieldUtil;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.concurrent.TimeUnit;

public class PostgreSQLReader extends AbstractReader {
    private Connection connection;
    private String logicalReplicationSlotName;
    private static final Logger logger = Logger.getLogger(Writer.class);

    public PostgreSQLReader() {
        dbType = DBTypeEnum.POSTGRESQL;
        // 默认逻辑复制插槽名称
        logicalReplicationSlotName = "test_slot";
    }

    @Override
    public void connect() {
        connection = ConnectUtil.connect(dbType, getReaderConfig().getUrl(), getReaderConfig().getUser(), getReaderConfig().getPassword());
        setFieldNames(FieldUtil.readFieldName(connection, getReaderConfig().getTableName()));
    }

    @Override
    public void read(Integer interval) {
        while (true) {
            readLogicalSlot(getSyncData());
            try {
                TimeUnit.MINUTES.sleep(interval);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    // 默认轮询间隔1分钟
    @Override
    public void read() {
        read(1);
    }

    // 读取逻辑复制插槽
    private void readLogicalSlot(SyncData syncData) {
        try {
            Statement statement = connection.createStatement();
            // 查看并消费插槽数据
            ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_logical_slot_get_changes('" + logicalReplicationSlotName + "', NULL, NULL)");
            ParseUtil.parsePGLogicalSlot(resultSet, syncData, getReaderConfig().getTableName());
        } catch (SQLException e) {
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

    // 自定义逻辑复制插槽名称
    public void setLogicalReplicationSlotName(String logicalReplicationSlotName) {
        this.logicalReplicationSlotName = logicalReplicationSlotName;
    }
}
