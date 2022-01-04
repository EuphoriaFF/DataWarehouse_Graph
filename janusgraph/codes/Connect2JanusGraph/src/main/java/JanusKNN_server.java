import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//服务器运行（未加时限）
public class JanusKNN_server {

    public static JanusGraph janusG;
    public static int steps = 1;
    public static GraphTraversalSource ts;

    public static void main(String[] args) {
        // args 0: property file
        String confPath = args[0];
        // args 1: root file
        String rootFile = args[1];
        // args 2: traversal depth
        steps = Integer.parseInt(args[2]);
        // args 3: how many roots to test
        int test_count = Integer.parseInt(args[3]);
        // args 4: result file name
        String resultFilePath = args[4];

        String resultFileFlag = args[5];
        // args 4: step size for uniform sampling
        int sample_step = 1;
        if(args.length >= 7){
            sample_step = Integer.parseInt(args[6]);
        }

        janusG = JanusGraphFactory.open(confPath);

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(rootFile)));
            FileWriter writer = new FileWriter(resultFilePath+"KN-graph500-"+steps+"_"+resultFileFlag+".csv");
            writer.write("start vertex,neighbor size,query count time (in ms),query next time (in ms)\n");
            writer.flush();

            String line = reader.readLine();
            reader.close();
            String[] roots = line.split(" ");

            ts = janusG.traversal();

            for(int i = 0; i < test_count; i += sample_step) {

                boolean error = false;
                long neighbor_size;
                long duration_count, duration_only_query;
                final String root = roots[i];
                ExecutorService executor = Executors.newSingleThreadExecutor();

                // handle query timeout
                long startTime = System.nanoTime();
                neighbor_size = ts.V().has("id", root).repeat(__.out("MyEdge")).times(steps).dedup().count().next();
                duration_count = System.nanoTime() - startTime;

                startTime = System.nanoTime();
                ts.V().has("id", root).repeat(__.out("MyEdge")).times(steps).next();
                duration_only_query = System.nanoTime() - startTime;

                writer.write(root + "," + neighbor_size + "," + duration_count/1000000.0+ "," + duration_only_query/1000000.0 + "\n");
                writer.flush();
                System.out.println(root + "," + Long.toString(neighbor_size) + "," + Double.toString((double)duration_count/1000000.0)+ "," + Double.toString((double)duration_only_query/1000000.0));
            }

            writer.flush();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.exit(0);

    }
}
