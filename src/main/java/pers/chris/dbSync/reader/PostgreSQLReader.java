package pers.chris.dbSync.reader;

import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.ResultSetUtil;
import pers.chris.dbSync.writer.Writer;
import pers.chris.dbSync.common.DBTypeEnum;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.concurrent.TimeUnit;

public class PostgreSQLReader extends Reader {
    private String logicalReplicationSlotName; // 逻辑复制槽名
    private static final Logger logger = Logger.getLogger(Writer.class);

    public PostgreSQLReader() {
        dbType = DBTypeEnum.POSTGRESQL;
        // 默认逻辑复制插槽名称
        logicalReplicationSlotName = "test_slot";
    }

    @Override
    public void read(SyncData syncData, Integer interval) {
        while (true) {
            readLogicalSlot(syncData);
            try {
                TimeUnit.MINUTES.sleep(interval);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    // 读取逻辑复制插槽
    private void readLogicalSlot(SyncData syncData) {
        try {
            Connection connection = getConnection();
            Statement statement = connection.createStatement();
            // 查看并消费插槽数据
            ResultSet resultSet = statement.executeQuery(
                    "SELECT * FROM pg_logical_slot_get_changes('"
                            + logicalReplicationSlotName
                            + "', NULL, NULL)");
            ResultSetUtil.parsePGSQLLogicalSlot(resultSet, syncData, getReaderConfig().getTableName(), getFields());
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public void setLogicalReplicationSlotName(String logicalReplicationSlotName) {
        this.logicalReplicationSlotName = logicalReplicationSlotName;
    }

}
