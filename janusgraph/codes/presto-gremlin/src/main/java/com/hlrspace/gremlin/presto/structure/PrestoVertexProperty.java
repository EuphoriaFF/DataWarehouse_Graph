package com.hlrspace.gremlin.presto.structure;

import com.hlrspace.gremlin.presto.connector.SessionManager;
import org.apache.commons.collections.map.HashedMap;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

//这个property是给Vertex用的
public class PrestoVertexProperty<V> extends PrestoProperty<V> implements VertexProperty<V>{


    public PrestoVertexProperty(Element element, String key, V value) {
        super(element, key, value);
    }

    @Override
    public Vertex element() {
        return (PrestoVertex) this.element;
    }

    @Override
    public Object id() {
        return this.key;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }
}
