package com.hlrspace.gremlin.presto.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

//这个property是给Edge用的
public class PrestoProperty<V> implements Property<V>{

    protected String key;
    protected V value;
    protected Element element;

    public PrestoProperty(Element element, String key, V value) {
        this.key = key;
        this.value = value;
        this.element = element;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public void remove() {
        throw Property.Exceptions.propertyRemovalNotSupported();
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
        return StringFactory.propertyString(this);
    }
}
