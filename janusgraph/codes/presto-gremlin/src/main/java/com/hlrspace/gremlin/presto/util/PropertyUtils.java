package com.hlrspace.gremlin.presto.util;

public class PropertyUtils {

    public static Class SQLTypeMapJavaType(String sqlType) {
        switch (sqlType) {
            case "text":
                return String.class;
            case "float":
                return Float.class;
            case "int":
                return Integer.class;
            default:
                throw new TypeNotPresentException(sqlType, null);
        }
    }
}
