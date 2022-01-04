package com.hlrspace.gremlin.presto;

import com.hlrspace.gremlin.presto.structure.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PrestoGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(PrestoEdge.class);
        add(PrestoElement.class);
        add(PrestoGraph.class);
        add(PrestoProperty.class);
        add(PrestoVertex.class);
        add(PrestoVertexProperty.class);
    }};

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        final String directory = makeTestDirectory(graphName, test, testMethodName);
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, PrestoGraph.class.getName());
            put(PrestoGraph.CONFIG_CATALOG, "cassandra");
            put(PrestoGraph.CONFIG_PORT, 8080);
            put(PrestoGraph.CONFIG_USER, "hlr");
            put(PrestoGraph.CONFIG_CONTACT_POINT, "127.0.0.1");
        }};
    }


    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        return;
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }
}
