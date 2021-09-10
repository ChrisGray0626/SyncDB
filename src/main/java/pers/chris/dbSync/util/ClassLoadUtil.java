package pers.chris.dbSync.util;

import pers.chris.dbSync.reader.Reader;
import pers.chris.dbSync.writer.Writer;
import pers.chris.dbSync.common.DBTypeEnum;

import java.util.HashMap;
import java.util.Map;

// 动态加载实现类工具
public class ClassLoadUtil {

    private static final Map<DBTypeEnum, String> map = new HashMap<>();

    static {
        map.put(DBTypeEnum.MYSQL, "MySQL");
        map.put(DBTypeEnum.POSTGRESQL, "PostgreSQL");
        map.put(DBTypeEnum.SQLSERVER, "SQLServer");
    }

    public static Writer loadWriterClass(DBTypeEnum dbType) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String basePath = Writer.class.getPackage().getName();
        String classPath = basePath + "." + map.get(dbType) + "Writer";
        Class<? extends Writer> writerClazz = (Class<? extends Writer>) Class.forName(classPath);
        return writerClazz.newInstance();
    }

    public static Reader loadReaderClass(DBTypeEnum dbType) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        String basePath = Reader.class.getPackage().getName();
        String classPath = basePath + "." + map.get(dbType) + "Reader";
        Class<? extends Reader> readerClazz = (Class<? extends Reader>) Class.forName(classPath);
        return readerClazz.newInstance();
    }
}
