package com.hlrspace.gremlin.presto.entity;

import java.util.Map;

public class NodeEdge {
    protected String id;
    protected String label;
    protected Node sourceNode;
    protected Node targetNode;
    protected Map<String, String> properties;

}
