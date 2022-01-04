package com.hlrspace.gremlin.presto.structure;

import com.hlrspace.gremlin.presto.connector.*;
import com.hlrspace.gremlin.presto.util.JDBCUtils;
import com.hlrspace.gremlin.presto.util.QueryUtils;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public final class PrestoVertex implements Vertex{

    private Object id;
    private String label;
    private Graph graph;
    private SessionManager sessionManager;


    private Map<String, Object> vertexInfoMap;
    private Map<String, Object> vertexLabelInfoMap;
    private Map<String, String> edges;


    public PrestoVertex(Object id, String label, Graph graph, SessionManager sessionManager) {
        this.id = id;
        this.label = label;
        this.graph = graph;
        this.sessionManager = sessionManager;
        init();
    }

    private void init() {
        Query vertexInfo = QueryUtils.all(this.label, (String) this.id);
        Query vertexLabelInfo = QueryUtils.vertexLabelInfo(this.label);
        try {
            vertexInfoMap = JDBCUtils.convertRresultSetToListMap(sessionManager.session().execute(vertexInfo)).get(0);
            vertexLabelInfoMap = JDBCUtils.convertRresultSetToListMap(sessionManager.session().execute(vertexLabelInfo)).get(0);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //下面初始化edges
        String edgesStr = (String)vertexInfoMap.get("edges");
        edges = Arrays.stream(edgesStr.substring(1, edgesStr.length() - 1).split(","))
                .collect(Collectors.toMap(
                        s -> s.substring(1, s.indexOf(":") - 1),
                        s -> s.substring(s.indexOf(":") + 2, s.length() - 1)
                ));

    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        Set<String>  edgeLabelsSet = new HashSet<>(Arrays.asList(edgeLabels));
        if(direction == Direction.IN) {
            return edges.entrySet().stream()
                    .filter(e -> e.getKey().indexOf((String)this.id) != 0)
                    .filter(e -> edgeLabelsSet.size() == 0 || edgeLabelsSet.contains(e.getValue()))
                    .map(e -> (Edge) new PrestoEdge(e.getKey(), e.getValue(), this.graph, this.sessionManager))
                    .collect(Collectors.toList())
                    .iterator();

        } else if(direction == Direction.OUT) {
            return edges.entrySet().stream()
                    .filter(e -> e.getKey().indexOf((String)this.id) == 0)
                    .filter(e -> edgeLabelsSet.size() == 0 || edgeLabelsSet.contains(e.getValue()))
                    .map(e -> (Edge) new PrestoEdge(e.getKey(), e.getValue(), this.graph, this.sessionManager))
                    .collect(Collectors.toList())
                    .iterator();

        } else {
            throw new UnsupportedOperationException("Edge direction error");
        }
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        Iterator<Edge> edgeIterator = edges(direction, edgeLabels);
        List<Vertex> vertexList = new ArrayList<>();
        if(direction == Direction.IN) {
            while (edgeIterator.hasNext())
                vertexList.add(edgeIterator.next().outVertex());
        }  else if(direction == Direction.OUT) {
            while (edgeIterator.hasNext())
                vertexList.add(edgeIterator.next().inVertex());
        } else {
            throw new UnsupportedOperationException("Edge direction error");
        }
        return vertexList.iterator();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        Set<String> propertyKeysSet = new HashSet<>(Arrays.asList(propertyKeys));
        //主要方案是将property转换成一个List，然后得到List的Iterator
        //edge_label表里面包含了关于一个label的所有信息，我们关心只是properties字段，并且把它存到一个list里
        //现在edgeInfoMap里面，就是id=？，weight=？这样的map的形式，需要把它们转换成Property对象
        String propertyString = (String)vertexLabelInfoMap.get("properties");
        //查出来的property虽然在cassandra里面是以list的形式存储的，
        // 但是由presto查出来，是以字符串形式返回的，所以需要一步步把它拆解为各个key
        return  Arrays.stream(
                        propertyString
                                .substring(1, propertyString.length() - 1)
                                .split(",")
                )
                .map(
                        s -> s.substring(1,s.length()-1)
                )
                .filter(s -> propertyKeysSet.size() == 0 || propertyKeysSet.contains(s))
                .map(
                        k -> (VertexProperty<V>) new PrestoVertexProperty<>(this, k, (V)vertexInfoMap.get(k))
                )
                .collect(Collectors.toList())
                .iterator();
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public void remove() {
        throw Vertex.Exceptions.vertexRemovalNotSupported();
    }


    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        throw Vertex.Exceptions.edgeAdditionsNotSupported();
    }


    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        throw new UnsupportedOperationException("Property add not supported");
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return ElementHelper.areEqual(this, obj);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
