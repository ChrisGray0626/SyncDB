package pers.chris.dbSync.util;

import pers.chris.dbSync.reader.Reader;
import pers.chris.dbSync.writer.Writer;
import pers.chris.dbSync.common.DBTypeEnum;

import java.util.HashMap;
import java.util.Map;

// 动态加载实现类工具
public class ClassLoadUtil {

    private ClassLoadUtil() {}

    private static final Map<DBTypeEnum, String> map;

    static {
        map = new HashMap<>();
        map.put(DBTypeEnum.MYSQL, "MySQL");
        map.put(DBTypeEnum.POSTGRESQL, "PostgreSQL");
        map.put(DBTypeEnum.SQLSERVER, "SQLServer");
    }

    public static Writer loadWriterClass(DBTypeEnum dbType) {
        String basePath = Writer.class.getPackage().getName();
        String classPath = basePath + "." + map.get(dbType) + "Writer";
        Writer writer = null;

        try {
            writer = (Writer) Class.forName(classPath).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return writer;
    }

    public static Reader loadReaderClass(DBTypeEnum dbType) {
        String basePath = Reader.class.getPackage().getName();
        String classPath = basePath + "." + map.get(dbType) + "Reader";
        Reader reader = null;

        try {
            reader = (Reader) Class.forName(classPath).newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return reader;
    }
}
