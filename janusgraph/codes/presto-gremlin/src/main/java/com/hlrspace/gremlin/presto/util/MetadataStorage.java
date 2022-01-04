package com.hlrspace.gremlin.presto.util;

import com.hlrspace.gremlin.presto.connector.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataStorage {
    private static Map<String, Class> propertyTypeMap = new HashMap<>();

    static {
        Query query = PrestoGraphQuery.builder()
                .select()
                .selector("*")
                .keyspace("presto_graph")
                .label("property")
                .build();

        //----------this is a temporary code snipets -----------
        Cluster prestoCluster = PrestoCluster.builder()
                .addContactPoint("127.0.0.1")
                .withPort(8080)
                .catalog("cassandra")
                .setProperty("user", "hlr")
                .build();
        Session session = prestoCluster.connect();
        SessionManager sessionManager = new PrestoSessionManager(session);
        ResultSet rs = sessionManager.session().execute(query);
        //------------------------------------------------------

        List<HashMap<String, Object>> list = null;
        try {
            list = JDBCUtils.convertRresultSetToListMap(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        list.stream().forEach(
                map -> propertyTypeMap.put(
                        (String) map.get("name"),
                        PropertyUtils.SQLTypeMapJavaType((String)map.get("type"))
                )
        );
    }

    //给定一个Property的名字，可以得到这个Property对应的在Java中的类型
    public static Class classOfPropertyKey(String propertyName) {
        return propertyTypeMap.get(propertyName);
    }
}
