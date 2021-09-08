package com.chris.util;

import com.chris.reader.AbstractReader;
import com.chris.writer.AbstractWriter;
import common.DBTypeEnum;

import java.util.HashMap;
import java.util.Map;

// 动态加载实现类工具
public class LoadClassUtil {

    private static final Map<DBTypeEnum, String> map = new HashMap<>();

    static {
        map.put(DBTypeEnum.MYSQL, "MySQL");
        map.put(DBTypeEnum.POSTGRESQL, "PostgreSQL");
        map.put(DBTypeEnum.SQLSERVER, "SQLServer");
    }

    public static AbstractWriter loadWriterClass(DBTypeEnum dbType) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String basePath = AbstractWriter.class.getPackage().getName();
        String classPath = basePath + "." + map.get(dbType) + "Writer";
        Class<? extends AbstractWriter> writerClazz = (Class<? extends AbstractWriter>) Class.forName(classPath);
        return writerClazz.newInstance();
    }

    public static AbstractReader loadReaderClass(DBTypeEnum readerType) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String basePath = AbstractReader.class.getPackage().getName();
        String classPath = basePath + "." + map.get(readerType) + "Reader";
        Class<? extends AbstractReader> readerClazz = (Class<? extends AbstractReader>) Class.forName(classPath);
        return readerClazz.newInstance();
    }
}
