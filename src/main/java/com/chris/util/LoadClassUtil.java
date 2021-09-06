package com.chris.util;

import com.chris.reader.AbstractReader;
import com.chris.reader.ReaderTypeEnum;
import com.chris.writer.AbstractWriter;
import com.chris.writer.WriterTypeEnum;

import java.util.HashMap;
import java.util.Map;

// 动态加载实现类工具
public class LoadClassUtil {

    private static final Map<WriterTypeEnum, String> writerNameMap = new HashMap<>();
    private static final Map<ReaderTypeEnum, String> readerNameType = new HashMap<>();

    static {
        writerNameMap.put(WriterTypeEnum.POSTGRESQL, "PostgreSQLWriter");

        readerNameType.put(ReaderTypeEnum.MYSQL, "MySQLReader");
        readerNameType.put(ReaderTypeEnum.POSTGRESQL, "PostgreSQLReader");
        readerNameType.put(ReaderTypeEnum.SQLSERVER, "SQLServerReader");
    }

    public static AbstractWriter getClass(WriterTypeEnum writerType) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String basePath = AbstractWriter.class.getPackage().getName();
        String classPath = basePath + "." + writerNameMap.get(writerType);
        Class<? extends AbstractWriter> writerClazz = (Class<? extends AbstractWriter>) Class.forName(classPath);
        return writerClazz.newInstance();
    }

    public static AbstractReader getClass(ReaderTypeEnum readerType) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String basePath = AbstractReader.class.getPackage().getName();
        String classPath = basePath + "." + readerNameType.get(readerType);
        Class<? extends AbstractReader> readerClazz = (Class<? extends AbstractReader>) Class.forName(classPath);
        return readerClazz.newInstance();
    }
}
