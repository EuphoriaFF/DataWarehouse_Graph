package com.hlrspace.gremlin.presto.structure;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactoryClass;

public class PrestoFactory {
    private PrestoFactory() {}

    public static PrestoGraph createTestGraph() {
        Configuration configuration = new BaseConfiguration();
        configuration.setProperty(PrestoGraph.CONFIG_CATALOG, "cassandra");
        configuration.setProperty(PrestoGraph.CONFIG_PORT, 8881);
        configuration.setProperty(PrestoGraph.CONFIG_CONTACT_POINT, "10.176.40.84");
        configuration.setProperty(PrestoGraph.CONFIG_USER, "hadoop");
        return PrestoGraph.open(configuration);

    }

    public static void main(String[] args) {
        PrestoGraph testGraph = createTestGraph();
    }

}
