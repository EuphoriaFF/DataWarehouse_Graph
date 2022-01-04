import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.io.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class janusKNN_simple {

    public static JanusGraph janusG;
    public static GraphTraversalSource ts;

    public static void main(String[] args) {

        janusG = JanusGraphFactory.build()
                .set("storage.backend","cql")
                .set("storage.batch-loading","true")
                .set("storage.cql.keyspace","graph500_qf_test")
                .set("storage.hostname","hadoop102")
                .set("ids.block-size","48000000")
                .set("storage.transactions","false")
                .set("storage.write-time","1000000 ms")
                .set("storage.cql.write-consistency-level","ANY")
                .set("storage.cql.read-consistency-level","ONE")
                .open();

        ts = janusG.traversal();

        System.out.println("============");

//        List<Vertex> ls = ts.V().has("id").toList();
//
//        System.out.println(ls.size());
//        for (Vertex v : ls) {
//            System.out.println("标签是======" + v.label() + "======名字是：" + v.value("id"));
//        }
//        Long cnt = ts.V().has("id", 1).out("MyEdge").out("MyEdge").dedup().count().next();

        Vertex next = ts.V().has("id", 1).out("MyEdge").next();
//        List<Vertex> vertices = ts.V().has("id", 2).out("MyEdge").out("MyEdge").toList();
//        for (Vertex vertex : vertices) {
//            Object id = vertex.value("id");
//            System.out.println("==========="+id);
//        }
//
//        GraphTraversal<Vertex, Vertex> out = ts.V().has("id", 2).out("MyEdge");
//
//
//        long v_cnt = ts.V().has("id", 2).out("MyEdge").count().next();

//        System.out.println(cnt);

        System.exit(0);

    }
}
