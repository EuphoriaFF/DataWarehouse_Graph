package com.hlrspace.gremlin.presto.structure;

import com.hlrspace.gremlin.presto.connector.Query;
import com.hlrspace.gremlin.presto.connector.SessionManager;
import com.hlrspace.gremlin.presto.util.JDBCUtils;
import com.hlrspace.gremlin.presto.util.QueryUtils;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class PrestoEdge extends PrestoElement implements Edge{

    private Map<String, Object> edgeInfoMap;
    private Map<String, Object> edgeLabelInfoMap;

    public PrestoEdge(Object id, String label, Graph graph, SessionManager sessionManager) {
        super(id, label, graph, sessionManager);
        init();
    }

    private void init() {
        Query info = QueryUtils.all(this.label, (String)this.id);
        Query keysQuery = QueryUtils.edgeLabelInfo(this.label);
        try {
            edgeInfoMap = JDBCUtils.convertRresultSetToListMap(sessionManager.session().execute(info)).get(0);
            edgeLabelInfoMap = JDBCUtils.convertRresultSetToListMap(sessionManager.session().execute(keysQuery)).get(0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        /**For a directed edge/arc/arrow 𝑒=(𝑢,𝑣),
         * which goes from 𝑢 to 𝑣, we call 𝑢 the tail(source) and 𝑣 the head(target),
         * exactly as you would draw an arrow from 𝑢 to 𝑣, i.e. 𝑢→𝑣.
         */
        String sourceLabel;
        String sourceId;
        if(direction == Direction.OUT) {
            sourceLabel = (String)edgeLabelInfoMap.get("source_label");
            sourceId = (String)edgeInfoMap.get("source_id");
        }
        else if(direction == Direction.IN) {
            sourceLabel = (String)edgeLabelInfoMap.get("target_label");
            sourceId = (String)edgeInfoMap.get("target_id");
        }
        else {
            throw new UnsupportedOperationException();
        }
        return Collections.singletonList((Vertex) new PrestoVertex(sourceId, sourceLabel, this.graph, sessionManager)).iterator();
    }

    @Override
    public void remove() {
        throw Edge.Exceptions.edgeRemovalNotSupported();
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        Set<String> propertyKeysSet = new HashSet<>(Arrays.asList(propertyKeys));
        //主要方案是将property转换成一个List，然后得到List的Iterator
        //edge_label表里面包含了关于一个label的所有信息，我们关心只是properties字段，并且把它存到一个list里
        //现在edgeInfoMap里面，就是id=？，weight=？这样的map的形式，需要把它们转换成Property对象
        String propertyString = (String)edgeLabelInfoMap.get("properties");
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
                        k -> (Property<V>) new PrestoProperty<>(this, k, (V)edgeInfoMap.get(k))
                )
                .collect(Collectors.toList())
                .iterator();

    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
