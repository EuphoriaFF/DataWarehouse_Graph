package com.hlrspace.gremlin.presto.connector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PrestoSession implements Session{
    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;

    public PrestoSession(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void query(Query query) {
        try {
            preparedStatement = connection.prepareStatement(query.query());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ResultSet execute() {
        try {
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return resultSet;
    }

    @Override
    public ResultSet execute(Query query) {
        query(query);
        return execute();
    }


    @Override
    public void close() {
        try {
            this.connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
