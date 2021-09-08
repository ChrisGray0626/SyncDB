package com.chris.reader;

import com.chris.util.ConnectUtil;
import com.chris.util.FieldUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;

public class Reader extends AbstractReader{

    private Connection connection;
    private static final Logger logger = Logger.getLogger(Reader.class);

    // TODO 统一的拉取方式
    @Override
    public void read(Integer interval) {

    }

    @Override
    public void read() {

    }

    @Override
    public void connect() {
        connection = ConnectUtil.connect(getReaderConfig().dbType, getReaderConfig().getUrl(), getReaderConfig().getUser(), getReaderConfig().getPassword());
        setFieldNames(FieldUtil.readFieldName(connection, getReaderConfig().getTableName()));
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
