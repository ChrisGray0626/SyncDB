package com.chris.util;

public class ClassPathUtil {

    public static String getClassPath(String basePath, String className) {
        return basePath + "." + className;
    }
}
