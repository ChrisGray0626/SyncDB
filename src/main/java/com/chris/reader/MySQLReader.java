package com.chris.reader;

import com.chris.config.ReaderConfig;
import com.chris.syncData.SyncData;
import com.chris.util.FieldsNameUtil;
import com.chris.util.ParseUtil;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Properties;

public class MySQLReader extends AbstractReader {

    public ReaderTypeEnum readerType;
    private SyncData syncData;
    private String hostname;
    private String port;
    private String url;
    private String user;
    private String password;
    private String databaseName;
    private String tableName;
    private String[] fieldsName;
    private Connection connection;
    private Statement statement;
    private BinaryLogClient client; // binlog监听客户端
    private static final Logger logger = Logger.getLogger(MySQLReader.class);

    public MySQLReader() {
        readerType = ReaderTypeEnum.MYSQL;
    }

    // TODO
    @Override
    public void config(String fileName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        databaseName = properties.getProperty("reader.databaseName");
        hostname = properties.getProperty("reader.hostname");
        port = properties.getProperty("reader.port");
        url = "jdbc:mysql://" + hostname + ":" + port + "/" + databaseName;
        user = properties.getProperty("reader.username");
        password = properties.getProperty("reader.password");
        tableName = properties.getProperty("reader.tableName");
    }

    @Override
    public void setSyncData(SyncData syncData) {
        this.syncData = syncData;
    }

    @Override
    public void setReaderConfig(ReaderConfig readerConfig) {

    }

    public void read(Integer interval) {
    }

    public void read() {
    }

    // 另外新建一条连接获取字段名
    public void setFieldsName() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(url, user, password);
            statement = connection.createStatement();
            fieldsName = FieldsNameUtil.getFieldsName(connection, tableName);
            connection.close();
        } catch (ClassNotFoundException | SQLException e) {
            logger.error(e);
        }
    }

    // 监听客户端配置
    private void clientConfig() {
        // 内部类使用需要，当前数据库名和表名
        final String[] curDataBaseName = {null};
        final String[] curTableName = {null};

        client = new BinaryLogClient(hostname, Integer.parseInt(port), user, password);
        EventDeserializer eventDeserializer = new EventDeserializer();

        // 序列号格式设置
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG
        );

        client.setEventDeserializer(eventDeserializer);

        client.registerEventListener(new BinaryLogClient.EventListener() {
            // 重写监听方法onEvent
            @Override
            public void onEvent(Event event) {
                EventData eventData = event.getData();

                // 获取数据库、表信息
                if (eventData instanceof TableMapEventData) {
                    TableMapEventData tableMapEventData = (TableMapEventData) eventData;

                    curDataBaseName[0] = tableMapEventData.getDatabase();
                    curTableName[0] = tableMapEventData.getTable();
                }
                // 获取事件类型INSERT的数据
                else if (eventData instanceof WriteRowsEventData) {
                    if (databaseName.equals(curDataBaseName[0])
                    && tableName.equals(curTableName[0])) {
                        WriteRowsEventData writeRowsEventData = (WriteRowsEventData) eventData;
                        List<List<String>> rowsData = ParseUtil.parseMySQLBinLogRows(writeRowsEventData.getRows());

                        syncData.setEventType(SyncData.EventTypeEnum.INSERT);
                        for (List<String> rows: rowsData) {
                            syncData.setRows(rows.toArray(new String[0]));
                        }

                    }
                }
            }
        });
    }

    @Override
    public void connect() {
        setFieldsName();
        clientConfig();
        try {
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {}
}
