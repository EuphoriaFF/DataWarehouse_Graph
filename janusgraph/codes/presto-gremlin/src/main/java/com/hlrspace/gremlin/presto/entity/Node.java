package com.hlrspace.gremlin.presto.entity;

import com.hlrspace.gremlin.presto.connector.PrestoGraphQuery;
import com.hlrspace.gremlin.presto.connector.Query;
import com.hlrspace.gremlin.presto.connector.SessionManager;
import com.hlrspace.gremlin.presto.structure.PrestoProperty;
import com.hlrspace.gremlin.presto.util.PropertyUtils;
import javafx.beans.property.Property;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class Node {
    protected String id;
    protected String label;
    protected Map<String, Property> properties;
    protected List<String> edgesIdList;
    protected List<NodeEdge> inEdges;
    protected List<NodeEdge> outEdges;
    protected SessionManager sessionManager;

    /*
    public <V> Property<V> property(String key) {
        String propertyType = "";
        //首先要决定这个property返回的类型是什么，所以需要先去property数据库查
        //select type from presto_graph.property where id = key;
        Query typeQuery = PrestoGraphQuery.builder()
                .select()
                .selector("type")
                .keyspace("presto_graph")
                .label("property")
                .where("name = '" + key + "'")
                .build();
        ResultSet resultSet = sessionManager.session().execute(typeQuery);
        try {
            resultSet.next();
            propertyType = resultSet.getString("type");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Class classOfValue = PropertyUtils.SQLTypeMapJavaType(propertyType);

        //select key from keyspace.label where id = this.id
        Query propertyValueQuery = PrestoGraphQuery.builder()
                .select()
                .selector(key)
                .keyspace("presto_graph")
                .label(this.label)
                .where("id = '" + this.id + "'")
                .build();
        ResultSet valueSet = sessionManager.session().execute(propertyValueQuery);
        Object propertyVal = null;
        try {
            valueSet.next();
            propertyVal = valueSet.getObject(key, classOfValue);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new PrestoProperty<>(key, propertyVal);
    }*/

}
