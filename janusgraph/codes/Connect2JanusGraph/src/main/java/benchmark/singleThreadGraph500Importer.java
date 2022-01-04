package benchmark;/*
* Copyright (c)  2015-now, TigerGraph Inc.
* All rights reserved
* It is provided as it is for benchmark reproducible purpose.
* anyone can use it for benchmark purpose with the
* acknowledgement to TigerGraph.
* Author: Weimo Liu weimo.liu@tigergraph.com
* Modified by: Litong Shen litong.shen@tigergraph.com
*/
import org.apache.commons.configuration.BaseConfiguration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import java.io.*;
import java.util.*;


public class singleThreadGraph500Importer {
	public static JanusGraph JanusG;
	public static int commitBatch = 1;

	private static HashMap<String, JanusGraphVertex> idset = new HashMap<String, JanusGraphVertex>();

	public static void main(String[] args){
		String datasetDir = args[0];
		String confPath = args[1];
		String fileResultName = args[2];
//		commitBatch = Integer.parseInt(args[2]);

//		String datasetDir = "/ebs/raw/graph500-22/graph500-22";
//		String confPath = "/opt/module/janusgraph-0.6.0/conf/myconf/graph500-janusgraph6.properties";
//		String fileResultName = "/opt/module/janusgraph-0.6.0/code/result/RunTimeGraph500_new4.txt";
//		String datasetDir = "/ebs/raw/graph500-22/graph500-22";
//		String confPath = "/ebs/install/janusgraph/conf/graph500-janusgraph-cassandra.properties";
//		String fileResultName = "/ebs/install/janusgraph/janusgraph-0.2.1-hadoop2/result/RunTimeGraph500_new3.txt";
		commitBatch = 4000;

		BaseConfiguration config = new BaseConfiguration();
	
		JanusG = JanusGraphFactory.open(confPath);
//		JanusG.close();
//		JanusGraphCleanup.clear(JanusG);
//		JanusG = JanusGraphFactory.open(confPath);

		ManagementSystem mgmt = (ManagementSystem) JanusG.openManagement();
		mgmt.makeEdgeLabel("MyEdge").make();
		mgmt.makeVertexLabel("MyNode").make();
		PropertyKey id_key = mgmt.makePropertyKey("id").dataType(String.class).make();
		//properties for pageRank

		PropertyKey pageRank_key = mgmt.makePropertyKey("gremlin.pageRankVertexProgram.pageRank").dataType(Double.class).make();
		PropertyKey edgeCount_key = mgmt.makePropertyKey("gremlin.pageRankVertexProgram.edgeCount").dataType(Long.class).make();	

		//properties for WCC

		PropertyKey groupId_key = mgmt.makePropertyKey("WCC.groupId").dataType(Long.class).make();		

		mgmt.buildIndex("byId", JanusGraphVertex.class).addKey(id_key).unique().buildCompositeIndex();
		mgmt.commit();
		try{
			mgmt.awaitGraphIndexStatus(JanusG, "byId").call();
		}
		catch(Exception ex) {
			ex.printStackTrace();
			System.exit(-1);
		}
		try {

			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(datasetDir)));
			String line;
			long lineCounter = 0;
			long startTime = System.nanoTime();
			while((line = reader.readLine()) != null) {
				try {
					String[] parts = line.split("\t");

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

			BufferedWriter write = new BufferedWriter(new FileWriter(new File(fileResultName)));
			write.write("######## loading time #######  " + Long.toString(duration/1000000) + " ms");
			write.close();
		}
		catch(IOException ioe) {
			ioe.printStackTrace();
		}
		System.out.println("---- done ----, total V: " + Integer.toString(idset.size()));
		System.exit(0);	
	}

	/** This function add vertex and edge
	* @param srcId the source vertex of the edge
	* @param dstId the destination vertex of the edge
	*/

	private static void processLine(String srcId, String dstId) {
		JanusGraphVertex srcVertex = (JanusGraphVertex)idset.get(srcId);
		JanusGraphVertex dstVertex = (JanusGraphVertex)idset.get(dstId);
		if(srcVertex == null) {
			Long groupId = Long.parseLong(srcId);

			srcVertex = JanusG.addVertex("MyNode");
			srcVertex.property("id", srcId);
			srcVertex.property("gremlin.pageRankVertexProgram.pageRank", 1.0);
			srcVertex.property("gremlin.pageRankVertexProgram.edgeCount", 0);
			srcVertex.property("WCC.groupId", groupId);
			
			idset.put(srcId, srcVertex);
		}
		if(dstVertex == null) {
			Long groupId = Long.parseLong(dstId);
			
			dstVertex = JanusG.addVertex("MyNode");
			dstVertex.property("id", dstId);
			dstVertex.property("gremlin.pageRankVertexProgram.pageRank", 1.0);
			dstVertex.property("gremlin.pageRankVertexProgram.edgeCount", 0);
			dstVertex.property("WCC.groupId", groupId);
			
			idset.put(dstId, dstVertex);
		}
		
		srcVertex.addEdge("MyEdge", dstVertex);
	}
	
	
}
