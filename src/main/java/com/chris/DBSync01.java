package com.chris;

import com.chris.reader.MySQLReader;
import com.chris.syncData.SyncData;
import com.chris.syncData.SyncDataEvent;
import com.chris.writer.PostgreSQLWriter;
import com.chris.writer.WriterTypeEnum;
import org.apache.log4j.Logger;


public class DBSync01 {

    private static final Logger logger = Logger.getLogger(DBSync01.class);

    public static void main(String[] args) {
        String configFileName = "resources/conf01.properties";
        SyncData syncData = new SyncData();
        PostgreSQLWriter postgreSQLWriter = new PostgreSQLWriter();
        MySQLReader mySQLReader = new MySQLReader();

        postgreSQLWriter.config(configFileName);
        postgreSQLWriter.init(syncData);
        postgreSQLWriter.connect();
        postgreSQLWriter.write();

        mySQLReader.config(configFileName);
        mySQLReader.init(syncData);
        mySQLReader.connect();
        mySQLReader.read();
    }
}
