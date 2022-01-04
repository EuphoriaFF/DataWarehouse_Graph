import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.*;
import org.janusgraph.core.*;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//服务器运行
public class JanusKNN_server_timeout {

    public static JanusGraph janusG;
    public static int steps = 1;
    public static GraphTraversalSource ts;
    public static FileWriter writer;

    public static void main(String[] args) throws InterruptedException {
        // args 0: property file
        String confPath = args[0];
        // args 1: root file
        String rootFile = args[1];
        // args 2: traversal depth
        steps = Integer.parseInt(args[2]);
        // args 3: 从哪个开始，一共测试多少个
        int test_start=Integer.parseInt(args[3]);
        int test_count = Integer.parseInt(args[4]);
        // args 4: result file name
        String resultFilePath = args[5];

        String resultFileFlag = args[6];

        String waitForTime=args[7];//程序运行时间（超过时终止）

        // args 4: step size for uniform sampling
        int sample_step = 1;

        if(args.length >= 9){
            sample_step = Integer.parseInt(args[7]);
        }

        janusG = JanusGraphFactory.open(confPath);

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(rootFile)));
            writer = new FileWriter(resultFilePath+"KN-graph500-"+steps+"_"+resultFileFlag+".csv");
            writer.write("start vertex,neighbor size,query count time (in ms),query next time (in ms)\n");
            writer.flush();

            String line = reader.readLine();
            reader.close();
            String[] roots = line.split(" ");

            ts = janusG.traversal();
            Object lock=new Object();

            for(int i = test_start; i < test_start+test_count; i += sample_step) {

                final String root = roots[i];
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

