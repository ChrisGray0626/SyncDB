package com.chris;

import com.chris.reader.AbstractReader;
import com.chris.syncData.SyncData;
import com.chris.util.ClassPathUtil;
import com.chris.writer.AbstractWriter;
import org.apache.log4j.Logger;

import java.util.Objects;

public class DBSync {

    private static final Logger logger = Logger.getLogger(DBSync.class);

    public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String configFileName = "resources/conf01.properties";

        SyncData syncData = new SyncData();
        syncData.config(configFileName);

        // TODO 类加载名称问题
        String writerClassPath = ClassPathUtil.getClassPath(AbstractWriter.class.getPackage().getName(), syncData.getWriterType());
        Class<? extends AbstractWriter> writerClazz = (Class<? extends AbstractWriter>) Class.forName(writerClassPath);
        AbstractWriter writer = writerClazz.newInstance();

        String readerClassPath = ClassPathUtil.getClassPath(AbstractReader.class.getPackage().getName(), syncData.getReaderType());
        Class<? extends AbstractReader> readerClazz = (Class<? extends AbstractReader>) Class.forName(readerClassPath);
        AbstractReader reader = readerClazz.newInstance();

        writer.config(configFileName);
        writer.init(syncData);
        writer.connect();
        writer.write();

        reader.config(configFileName);
        reader.initSyncData(syncData);
        reader.connect();
        reader.read();

    }
}
