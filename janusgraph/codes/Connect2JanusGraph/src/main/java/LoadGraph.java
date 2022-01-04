import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import java.io.*;
import java.util.HashMap;

public class LoadGraph {

    public static JanusGraph JanusG;
    public static GraphTraversalSource ts;
    public static int commitBatch = 1;

    private static HashMap<String, JanusGraphVertex> idset = new HashMap<String, JanusGraphVertex>();

    public static void main(String[] args) {
        JanusG = JanusGraphFactory.build()
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

        String datasetDir = "\\data\\graph500_small";

        commitBatch = 3;

        ManagementSystem mgmt = (ManagementSystem) JanusG.openManagement();
        mgmt.makeEdgeLabel("MyEdge").make();
        mgmt.makeVertexLabel("MyNode").make();
        PropertyKey id_key = mgmt.makePropertyKey("id").dataType(String.class).make();
        //properties for pageRank

        mgmt.buildIndex("byId", JanusGraphVertex.class).addKey(id_key).unique().buildCompositeIndex();
        mgmt.commit();
//        try{
//            mgmt.awaitGraphIndexStatus(JanusG, "byId").call();
//        }
//        catch(Exception ex) {
//            ex.printStackTrace();
//            System.exit(-1);
//        }
        try {

            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
            String line;
            long lineCounter = 0;
            long startTime = System.nanoTime();
            while((line = reader.readLine()) != null) {
                try {
                    String[] parts = line.split(" ");
//                    System.out.println(parts[0]+","+parts[1]);

                    processLine(parts[0], parts[1]);

                    lineCounter++;
                    if(lineCounter % commitBatch == 0){
                        System.out.println("---- commit ----: " + Long.toString(lineCounter / commitBatch));
                        JanusG.tx().commit();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            JanusG.tx().commit();
            long endTime = System.nanoTime();
            long duration = (endTime - startTime);
            System.out.println("######## loading time #######  " + Long.toString(duration/1000000) + " ms");
            reader.close();
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        System.out.println("---- done ----, total V: " + Integer.toString(idset.size()));
        System.exit(0);
    }

    private static void processLine(String srcId, String dstId) {
        JanusGraphVertex srcVertex = (JanusGraphVertex)idset.get(srcId);
        JanusGraphVertex dstVertex = (JanusGraphVertex)idset.get(dstId);
        if(srcVertex == null) {
            srcVertex = JanusG.addVertex("MyNode");
            srcVertex.property("id", srcId);

            idset.put(srcId, srcVertex);
        }
        if(dstVertex == null) {
            dstVertex = JanusG.addVertex("MyNode");
            dstVertex.property("id", dstId);

            idset.put(dstId, dstVertex);
        }

        srcVertex.addEdge("MyEdge", dstVertex);
    }
}


