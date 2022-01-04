package com.hlrspace.gremlin.presto.connector;

public class PrestoSessionManager implements SessionManager{
    Session session;

    public PrestoSessionManager(Session session) {
        this.session = session;
    }

    @Override
    public Session session() {
        return this.session;
    }
}
