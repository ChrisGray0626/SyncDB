package com.chris.util;

import com.chris.reader.AbstractReader;
import com.chris.reader.ReaderTypeEnum;
import com.chris.writer.AbstractWriter;
import com.chris.writer.WriterTypeEnum;

import java.util.HashMap;
import java.util.Map;

public class ClassPathUtil {

    // 动态加载类名的获取类名工具
    public static String getWriterClassPath(WriterTypeEnum writer) {
        String basePath = AbstractWriter.class.getPackage().getName();
        Map<WriterTypeEnum, String> map = new HashMap<>();

        map.put(WriterTypeEnum.POSTGRESQL, "PostgreSQLWriter");

        return basePath + "." + map.get(writer);
    }

    public static String getReaderClassPath(ReaderTypeEnum reader) {
        String basePath = AbstractReader.class.getPackage().getName();
        Map<ReaderTypeEnum, String> map = new HashMap<>();

        map.put(ReaderTypeEnum.MYSQL, "MySQLReader");
        map.put(ReaderTypeEnum.POSTGRESQL, "PostgreSQLReader");
        map.put(ReaderTypeEnum.SQLSERVER, "SQLServerReader");

        return basePath + "." + map.get(reader);
    }
}
