package com.chris;

import com.chris.config.Config;
import com.chris.config.ReaderConfig;
import com.chris.config.SyncDataConfig;
import com.chris.config.WriterConfig;
import com.chris.reader.AbstractReader;
import com.chris.syncData.SyncData;
import com.chris.util.LoadClassUtil;
import com.chris.writer.AbstractWriter;
import org.apache.log4j.Logger;


public class DBSync {

    private static final Logger logger = Logger.getLogger(DBSync.class);

    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String configFileName = "resources/conf.properties";
        Config config = new Config();
        config.config(configFileName);
        config.getConfigInfo();
        WriterConfig writerConfig = config.getWriterConfig();
        ReaderConfig readerConfig = config.getReaderConfig();
        SyncDataConfig syncDataConfig = config.getSyncDataConfig();

        SyncData syncData = new SyncData();
        syncData.setSyncDataConfig(syncDataConfig);

        // 动态加载Writer、Reader
        AbstractWriter writer = LoadClassUtil.getClass(writerConfig.getWriterType());
        AbstractReader reader = LoadClassUtil.getClass(readerConfig.getReaderType());

        writer.setWriterConfig(writerConfig);
        writer.setSyncData(syncData);
        writer.connect();
        writer.write();

        reader.setReaderConfig(readerConfig);
        reader.setSyncData(syncData);
        reader.connect();
        reader.read();
    }
}
