package com.chris;

import com.chris.configuration.Configuration;
import com.chris.configuration.ReaderConfiguration;
import com.chris.configuration.SyncDataConfiguration;
import com.chris.configuration.WriterConfiguration;
import com.chris.reader.AbstractReader;
import com.chris.syncData.SyncData;
import com.chris.syncData.SyncDataEvent;
import com.chris.util.LoadClassUtil;
import com.chris.writer.AbstractWriter;
import org.apache.log4j.Logger;


public class DBSync {

    private static final Logger logger = Logger.getLogger(DBSync.class);

    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String configFileName = "resources/conf.properties";
        Configuration configuration = new Configuration();
        configuration.setConfigId("1");
        configuration.config(configFileName);
        configuration.configInfo();
        WriterConfiguration writerConfiguration = configuration.getWriterConfig();
        ReaderConfiguration readerConfiguration = configuration.getReaderConfig();
        SyncDataConfiguration syncDataConfiguration = configuration.getSyncDataConfig();

        SyncData syncData = new SyncData();
        syncData.setSyncDataConfig(syncDataConfiguration);

        // 动态加载Writer、Reader
        AbstractWriter writer = LoadClassUtil.loadWriterClass(writerConfiguration.getDBType());
        AbstractReader reader = LoadClassUtil.loadReaderClass(readerConfiguration.getDBType());

        syncData.registerListener(new SyncData.SyncDataListener() {
            @Override
            public void doSet(SyncDataEvent event) {
                writer.write(syncData);
            }
        });

        writer.setWriterConfig(writerConfiguration);
        writer.setSyncData(syncData);
        writer.connect();

        reader.setReaderConfig(readerConfiguration);
        reader.setSyncData(syncData);
        reader.connect();
        reader.read();
    }
}
