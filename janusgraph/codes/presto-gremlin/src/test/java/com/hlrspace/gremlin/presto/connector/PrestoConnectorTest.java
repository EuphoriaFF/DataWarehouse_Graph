package com.hlrspace.gremlin.presto.connector;


import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class PrestoConnectorTest {

    @Test
    public void testPrestoSession() {
        /*
        Cluster prestoCluster = PrestoCluster.builder()
                .addContactPoint("127.0.0.1")
                .withPort(8080)
                .catalog("cassandra")
                .setProperty("user", "hlr")
                .build();
        Session session = prestoCluster.connect();
        Query query = PrestoGraphQuery.builder().select().keyspace("node").label("person").selector("id").where("id = 'okram'").build();
        session.query(query);
        ResultSet resultSet = session.execute();
        try {
            resultSet.next();
            assertEquals("okram", resultSet.getString("id"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
         */
    }

    @Test
    public void testPrestoGraphQuery() {
        /*
        Query queryTest0 = PrestoGraphQuery.builder().label("person").keyspace("node").selector("id").build();
        assertEquals("select id from node.person", queryTest0.query());
        Query queryTest1 = PrestoGraphQuery.builder().label("supports").keyspace("edge").selector("id", "key", "value").where("id = test").build();
        assertEquals("select id, key, value from edge.supports where id = test", queryTest1.query());
        */
    }

}
