package com.chris;

import com.chris.reader.AbstractReader;
import com.chris.syncData.SyncData;
import com.chris.util.ClassPathUtil;
import com.chris.writer.AbstractWriter;
import org.apache.log4j.Logger;


public class DBSync {

    private static final Logger logger = Logger.getLogger(DBSync.class);

    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String configFileName = "resources/conf01.properties";
        SyncData syncData = new SyncData();
        syncData.config(configFileName);

        // 动态加载Writer、Reader
        String writerClassPath = ClassPathUtil.getWriterClassPath(syncData.getWriterType());
        Class<? extends AbstractWriter> writerClazz = (Class<? extends AbstractWriter>) Class.forName(writerClassPath);
        AbstractWriter writer = writerClazz.newInstance();

        String readerClassPath = ClassPathUtil.getReaderClassPath(syncData.getReaderType());
        Class<? extends AbstractReader> readerClazz = (Class<? extends AbstractReader>) Class.forName(readerClassPath);
        AbstractReader reader = readerClazz.newInstance();

        writer.config(configFileName);
        writer.setSyncData(syncData);
        writer.connect();
        writer.setFieldsName();
        writer.write();

        reader.config(configFileName);
        reader.setSyncData(syncData);
        reader.connect();
        reader.setFieldsName();
        reader.read(syncData.getInterval());

    }
}
