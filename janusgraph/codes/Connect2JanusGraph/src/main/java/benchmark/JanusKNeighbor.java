package benchmark;/*
 * Copyright (c)  2015-now, TigerGraph Inc.
 * All rights reserved
 * It is provided as it is for benchmark reproducible purpose.
 * anyone can use it for benchmark purpose with the
 * acknowledgement to TigerGraph.
 * Author: Litong Shen litong.shen@tigergraph.com
 */
import java.util.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.*;
import org.apache.tinkerpop.gremlin.structure.Graph;

import org.janusgraph.core.schema.*;
import org.janusgraph.core.util.*;
import org.janusgraph.core.*;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.management.*;

import java.io.FileWriter;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JanusKNeighbor {
    public static JanusGraph janusG;
    public static int steps = 1;
    public static GraphTraversalSource ts;

    public static void main(String[] args){
        // args 0: property file
        String confPath = args[0];
        // args 1: root file
        String rootFile = args[1];
        // args 2: traversal depth
        steps = Integer.parseInt(args[2]);
        // args 3: how many roots to test
        int test_count = Integer.parseInt(args[3]);
        // args 4: step size for uniform sampling
        int sample_step = 1;
        if(args.length >= 5){
            sample_step = Integer.parseInt(args[4]);
        }

        janusG = JanusGraphFactory.open(confPath);
        BufferedReader reader = null;
        try {
            // initialize output file
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(rootFile)));
            String resultFileName = "KN-latency-graph500-Traversal-" + steps +"-"+test_count;
            String resultFilePath = "/ebs/benchmark/code/janusgraph/result/" + resultFileName;
            FileWriter writer = new FileWriter(resultFilePath);
            writer.write("start vertex,\tneighbor size,\tquery time (in ms)\n");
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
                neighbor_size = ts.V().has("id", root).repeat(__.out("MyEdge")).times(steps).dedup().count().next();
                duration = System.nanoTime() - startTime;

                writer.write(root + ",\t" + neighbor_size + ",\t" + duration/1000000.0 + "\n");
                writer.flush();
                System.out.println(root + "," + Long.toString(neighbor_size) + "," + Double.toString((double)duration/1000000.0));
            }

            writer.flush();
            writer.close();
        }catch(IOException ioe) {
            ioe.printStackTrace();
        }
        System.exit(0);
    }

    /** this funtion calculate k-hop distinct neighbor size
     * return k-hop distinct neighbor size
     * @para root the start vertex
     */
    private static Long runKNeighbor(String root) {
        return ts.V().has("id", root).repeat(__.out("MyEdge")).times(steps).dedup().count().next();
    }
}
