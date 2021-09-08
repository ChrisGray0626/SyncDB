package com.chris.util;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FieldUtil {

    private static final Logger logger = Logger.getLogger(FieldUtil.class);

    public static String[] readFieldName(Connection connection, String tableName) {
        List<String> fieldsName = new ArrayList<>();

        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getColumns(null, "%", tableName, "%");

            while (resultSet.next()) {
                fieldsName.add(resultSet.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            logger.error(e);
        }
        return fieldsName.toArray(new String[0]);
    }
}
