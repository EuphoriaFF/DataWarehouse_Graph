import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.io.*;

//
public class JanusKNN_local_timeout {

    public static JanusGraph janusG;
    public static int steps = 1;
    public static GraphTraversalSource ts;
    public static FileWriter writer;

    public static void main(String[] args) throws InterruptedException {
        String rootFile = "\\data\\graph500-22-seed";
        // args 2: traversal depth
        steps = 3;
        // args 3: how many roots to test
        int test_count = 7;
        // args 4: result file name
        String resultFileName = "KN-graph500-" + steps +".csv";
        String resultFilePath = "\\result\\" + resultFileName;
        String waitForTime="30000";

        int sample_step = 1;

        janusG = JanusGraphFactory.build()
                .set("storage.backend","cql")
                .set("storage.batch-loading","true")
                .set("storage.cql.keyspace","graph500_qf_new2")
                .set("storage.hostname","10.176.40.84, 10.176.40.85, 10.176.40.87")
                .set("ids.block-size","48000000")
                .set("storage.transactions","false")
                .set("storage.write-time","1000000 ms")
                .set("storage.cql.write-consistency-level","ANY")
                .set("storage.cql.read-consistency-level","ONE")
                .open();

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(rootFile)));
            writer = new FileWriter(resultFilePath);
            writer.write("start vertex,neighbor size,query count time (in ms),query next time (in ms)\n");
            writer.flush();

            String line = reader.readLine();
            reader.close();
            String[] roots = line.split(" ");

            ts = janusG.traversal();
            Object lock=new Object();

            for(int i = 3; i < test_count; i += sample_step) {

                final String root = roots[i];
                System.out.println("root================"+root);
                synchronized (lock){
                    Thread t1=new Thread(new Query(root));
                    t1.start();

                    t1.join(Long.parseLong(waitForTime));
                    t1.interrupt();
                }

            }
            Thread.sleep(2000);

            writer.flush();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.exit(0);

    }

    public static class Query implements Runnable{

        private String root;

        public Query(String root) {
            this.root = root;
        }

        public void run() {
            while (true){
                Thread current = Thread.currentThread();
                if(current.isInterrupted()){
                    try {
                        writer.write(root + ",0,0\n");
                        writer.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    break;
                }
                try{
                    long startTime = System.nanoTime();
                    long neighbor_size = ts.V().has("id", root).repeat(__.out("MyEdge")).times(steps).dedup().count().next();
                    long duration_count = System.nanoTime() - startTime;

                    try {
                        writer.write(root + "," + neighbor_size + "," + duration_count/1000000.0 + "\n");
                        writer.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println(root + "," + Long.toString(neighbor_size) + "," + Double.toString((double)duration_count/1000000.0));
                    break;
                }catch (Exception e){
                    current.interrupt();
                }
            }
        }
    }
}

