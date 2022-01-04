package com.hlrspace.gremlin.presto.connector;

import java.sql.ResultSet;

public interface Session {

    public void query(Query query);

    public ResultSet execute();

    public ResultSet execute(Query query);

    public void close();

}
