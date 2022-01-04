package GodTest;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.example.GraphOfTheGodsFactory;

import java.util.Map;

import static org.janusgraph.core.attribute.Geo.geoWithin;


public class GodsTest {

    public static void main(String[] args) throws Exception {
        JanusGraph graph = JanusGraphFactory.build()
                .set("storage.backend", "cql")
                .set("storage.hostname", "hadoop102")
                .open();

//        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph, true);

        GraphTraversalSource g = graph.traversal();

        Vertex saturn = g.V().has("name", "saturn").next();
        Object next = g.V(saturn).in("father").in("father").values("name").next();

        System.out.println(g.E().has("place", geoWithin(Geoshape.circle(37.97, 23.72, 50))));
        System.out.println(g.E().has("place", geoWithin(Geoshape.circle(37.97, 23.72, 50)))
                .as("source").inV()
                .as("god2")
                .select("source").outV()
                .as("god1").select("god1", "god2")
                .by("name"));

        g.close();
        graph.close();

    }
}
