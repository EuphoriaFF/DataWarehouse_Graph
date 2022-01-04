package com.hlrspace.gremlin.presto.structure;

import com.hlrspace.gremlin.presto.connector.PrestoGraphQuery;
import com.hlrspace.gremlin.presto.connector.Query;
import com.hlrspace.gremlin.presto.connector.Session;
import com.hlrspace.gremlin.presto.connector.SessionManager;
import com.hlrspace.gremlin.presto.util.PropertyUtils;
import com.hlrspace.gremlin.presto.util.QueryUtils;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

public abstract class PrestoElement implements Element{

    protected Object id;
    protected String label;
    protected Graph graph;
    protected SessionManager sessionManager;

    public PrestoElement(Object id, String label, Graph graph, SessionManager sessionManager) {
        this.id = id;
        this.label = label;
        this.graph = graph;
        this.sessionManager = sessionManager;
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
    public <V> Property<V> property(String key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return ElementHelper.areEqual(this, obj);
    }

}
