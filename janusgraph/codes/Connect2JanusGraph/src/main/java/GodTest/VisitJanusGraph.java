package GodTest;

import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class VisitJanusGraph {

    public static void main(String[] args) throws Exception {
        JanusGraph graph = JanusGraphFactory.build()
                .set("storage.backend", "cql")
                .set("storage.hostname", "hadoop102")
                .open();

        GraphTraversalSource g = graph.traversal();

//        Vertex saturn = g.V().has("name", "saturn").next();

        System.out.println("========== ok =============");


//        g.addV("person").property("name", "John").next();
//        g.addV("person").property("name", "Snow").next();
//        g.addV("person").property("name", "Erya").next();
//        g.tx().commit();

//        System.out.println("Vertex count = " + g.V().count().next());
//        System.out.println("Edges count = " + g.E().count().next());
//
//        System.out.println("++++++++图结构+++++++++" + graph.openManagement().printSchema());
//
//        List<Vertex> ls = g.V().has("name").toList();
//
//
//        for (Vertex v : ls) {
//            System.out.println("标签是======" + v.label() + "======名字是：" + v.value("name"));
//        }

        g.close();
        graph.close();

    }
}