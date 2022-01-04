package com.hlrspace.gremlin.presto.jsr223;

import com.hlrspace.gremlin.presto.structure.*;
import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;

public class PrestoGremlinPlugin extends AbstractGremlinPlugin {

    private static final String NAME = "hlr.presto-graph";

    private static final ImportCustomizer imports;

    static {
        try {
            imports = DefaultImportCustomizer.build()
                    .addClassImports(PrestoEdge.class,
                            PrestoElement.class,
                            PrestoGraph.class,
                            PrestoProperty.class,
                            PrestoVertex.class,
                            PrestoVertexProperty.class)
                    .create();
        } catch ( Exception e) {
            throw new RuntimeException();
        }
    }

    private static final PrestoGremlinPlugin instance = new PrestoGremlinPlugin();

    public PrestoGremlinPlugin() {
        super (NAME, imports);
    }

    public static PrestoGremlinPlugin instance() {
        return instance;
    }

}
