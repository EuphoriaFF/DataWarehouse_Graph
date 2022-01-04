package com.hlrspace.gremlin.presto.connector;

import java.sql.*;
import java.util.Properties;

public class PrestoCluster implements Cluster{

    private Properties properties;
    private String catalog;
    private String connectUrl;

    //String connect_url = "jdbc:presto://192.168.38.5:8080";

    private PrestoCluster() {}

    private PrestoCluster(Builder builder) {
        this.properties = builder.properties;
        connectUrl = "jdbc:presto://" + builder.address + ":" + Integer.toString(builder.port) + "/" + builder.catalog;
    }

    public PrestoSession connect() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectUrl, properties);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new PrestoSession(connection);
    }


    /*
    public void query() {
        try (Connection connection = DriverManager.getConnection(connectUrl, properties)) {
            PreparedStatement preparedStatement = connection.prepareStatement("select * from node.person");

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    System.out.println(resultSet.getString("id"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    */


    public static Builder builder() {return new Builder();}

    public static final class Builder {
        private String address;
        private int port;
        private Properties properties = new Properties();
        private String catalog;

        public Builder() {}

        public Builder addContactPoint(String address) {
            this.address = address;
            return this;
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setProperty(String key, String value) {
            this.properties.setProperty(key, value);
            return this;
        }

        public Builder catalog(String catalog) {
            this.catalog = catalog;
            return this;
        }

        public PrestoCluster build() {
            return new PrestoCluster(this);
        }
    }
}
