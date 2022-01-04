package com.hlrspace.gremlin.presto.util;

import com.hlrspace.gremlin.presto.connector.PrestoGraphQuery;
import com.hlrspace.gremlin.presto.connector.Query;

public class QueryUtils {
    //这个是为了查出某个property代表的Cassandra类型
    public static Query propertyType(String propertyName) {
        Query typeQuery = PrestoGraphQuery.builder()
                .select()
                .selector("type")
                .keyspace("presto_graph")
                .label("property")
                .where("name = '" + propertyName + "'")
                .build();
        return typeQuery;
    }

    //查出某个property的value
    public static Query propertyValue(String key, String label, String id) {
        Query propertyValueQuery = PrestoGraphQuery.builder()
                .select()
                .selector(key)
                .keyspace("presto_graph")
                .label(label)
                .where("id = '" + id + "'")
                .build();
        return propertyValueQuery;
    }

    //查出一个边所有的和label有关的信息
    public static Query edgeLabelInfo(String label) {
        Query edgeKeysQuery = PrestoGraphQuery.builder()
                .select()
                .selector("*")
                .keyspace("presto_graph")
                .label("edge_label")
                .where("name = '" + label +"'")
                .build();
        return edgeKeysQuery;

    }

    //查出一个节点的所有label有关的信息
    public static Query vertexLabelInfo(String label) {
        Query vertexLabelQuery = PrestoGraphQuery.builder()
                .select()
                .selector("*")
                .keyspace("presto_graph")
                .label("vertex_label")
                .where("name = '" + label + "'")
                .build();
        return vertexLabelQuery;
    }

    //查处出一个节点的所有信息 select * from
    public static Query all(String label, String id) {
        Query all = PrestoGraphQuery.builder()
                .select()
                .selector("*")
                .keyspace("presto_graph")
                .label(label)
                .where("id = '" + id + "'")
                .build();
        return all;
    }

    //查出所有的edge label
    public static Query edgeLabel() {
        Query edgeLabel = PrestoGraphQuery.builder()
                .nativeSQL("select name from presto_graph.edge_label")
                .build();
        return edgeLabel;
    }

    //查出所有的vertex label
    public static Query vertexLabel() {
        Query vertexLabel = PrestoGraphQuery.builder()
                .nativeSQL("select name from presto_graph.vertex_label")
                .build();
        return vertexLabel;
    }

    //查出某个id下的所有label
    public static Query idByLabel(String label) {
        Query query = PrestoGraphQuery.builder()
                .nativeSQL("select id from presto_graph."+label)
                .build();
        return query;
    }

}
