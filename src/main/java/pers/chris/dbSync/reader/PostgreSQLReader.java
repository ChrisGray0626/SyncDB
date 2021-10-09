package pers.chris.dbSync.reader;

import pers.chris.dbSync.common.typeEnum.EventTypeEnum;
import pers.chris.dbSync.syncData.SyncData;
import pers.chris.dbSync.util.FieldUtil;
import pers.chris.dbSync.writer.Writer;
import pers.chris.dbSync.common.typeEnum.DBTypeEnum;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostgreSQLReader extends Reader {
    private String logicalReplicationSlotName; // 逻辑复制槽名
    private static final Logger logger = Logger.getLogger(Writer.class);

    public PostgreSQLReader() {
        dbType = DBTypeEnum.POSTGRESQL;
        // 默认逻辑复制插槽名称
        logicalReplicationSlotName = "test_slot";
    }

    @Override
    public void read() {
        while (true) {
            readLogicalSlot();
            try {
                TimeUnit.MINUTES.sleep(getSyncConf().interval);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    // 读取逻辑复制插槽
    private void readLogicalSlot() {
        try {
            Connection connection = getConnection();
            Statement statement = connection.createStatement();
            // 查看并消费插槽数据
            ResultSet resultSet = statement.executeQuery(
                    "SELECT * FROM pg_logical_slot_get_changes('"
                            + logicalReplicationSlotName
                            + "', NULL, NULL)");

            while (resultSet.next()) {
                SyncData syncData = new SyncData();
                // 获取原始数据
                String[] originalData = resultSet.getString("data").split(" ");
                // 非table开头的不是事件数据
                if (!"table".equals(originalData[0])) {
                    continue;
                }

                // 解析原始数据
                String[] originalData1 = originalData[1].split("\\.");
                String schema = originalData1[0];
                String curTableName = originalData1[1].replace(":", "");
                EventTypeEnum eventType = EventTypeEnum.valueOf(originalData[2].replace(":", ""));

                if (!curTableName.equals(getReaderConf().tableName)) {
                    continue;
                }

                syncData.eventType = eventType;
                switch (eventType) {
                    case INSERT:
                        List<String> values = new ArrayList<>();
                        // 正则匹配数据
                        Pattern pattern = Pattern.compile("(?<=:)(('.*?')|([\\S]+))");
                        Matcher matcher = pattern.matcher(resultSet.getString("data"));
                        while (matcher.find()) {
                            values.add(matcher.group().replace("'", ""));
                        }
                        syncData.setData(FieldUtil.mergeFieldAndValue(getFieldNames(), values));
                        super.trigger(syncData);
                        break;
                    default:
                }
            }
        } catch (SQLException e) {
            logger.error(e);
        }
    }

    public void setLogicalReplicationSlotName(String logicalReplicationSlotName) {
        this.logicalReplicationSlotName = logicalReplicationSlotName;
    }

}
