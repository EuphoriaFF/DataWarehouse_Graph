package com.hlrspace.gremlin.presto.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;
import java.util.Set;

public class PrestoGraphVariable implements Graph.Variables {

    @Override
    public Set<String> keys() {
        return null;
    }

    @Override
    public <R> Optional<R> get(String key) {
        return Optional.empty();
    }

    @Override
    public void set(String key, Object value) {

    }

    @Override
    public void remove(String key) {

    }

}
