import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JanusKNN_local {

    public static JanusGraph janusG;
    public static GraphTraversalSource ts;

    public static void main(String[] args) {
        // args 1: 查询节点
        String rootFile = "\\data\\graph500-22-seed";
        // args 2: traversal depth
        int steps = 1;
        // args 3: 查询节点个数
        int test_count = 100;
        // args 4: step size for uniform sampling
        int sample_step = 1;

//        janusG = JanusGraphFactory.open(confPath);

        janusG = JanusGraphFactory.build()
                .set("storage.backend","cql")
                .set("storage.batch-loading","true")
                .set("storage.cql.keyspace","graph500_qf_new2")
                .set("storage.hostname","10.176.40.84, 10.176.40.85, 10.176.40.87")
                .set("ids.block-size","48000000")
                .set("storage.transactions","false")
                .set("storage.write-time","1000000 ms")
                .set("storage.cql.write-consistency-level","ANY")
                .set("storage.cql.read-consistency-level","QUORUM")
                .open();

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(rootFile)));
            String resultFileName = "KN-graph500-" + steps +".csv";
            String resultFilePath = "\\result\\" + resultFileName;
            FileWriter writer = new FileWriter(resultFilePath);
            writer.write("start vertex,neighbor size,query time (in ms)\n");
            writer.flush();

            String line = reader.readLine();
            reader.close();
            String[] roots = line.split(" ");

            ts = janusG.traversal();

            for(int i = 0; i < test_count; i += sample_step) {

                boolean error = false;
                long neighbor_size;
                long duration;
                final String root = roots[i];
                ExecutorService executor = Executors.newSingleThreadExecutor();

                // handle query timeout
                long startTime = System.nanoTime();
//                neighbor_size = ts.V().has("id", root).repeat(__.out("MyEdge")).times(steps).dedup().count().next();
                Vertex next = ts.V().has("id", root).repeat(__.out("MyEdge")).times(steps).next();
                String neighbor_v = next.toString();
                duration = System.nanoTime() - startTime;

                writer.write(root + "," + neighbor_v + "," + duration/1000000.0 + "\n");
                writer.flush();
                System.out.println(root + "," + neighbor_v + "," + Double.toString((double)duration/1000000.0));
            }

            writer.flush();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.exit(0);

    }
}
