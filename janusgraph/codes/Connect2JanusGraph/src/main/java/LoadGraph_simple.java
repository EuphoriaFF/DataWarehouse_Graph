/*
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
import java.util.HashMap;


public class LoadGraph_simple {
	public static JanusGraph JanusG;
	public static int commitBatch = 1;

	private static HashMap<String, JanusGraphVertex> idset = new HashMap<String, JanusGraphVertex>();

	public static void main(String[] args){
		String datasetDir = args[0];
		String confPath = args[1];
		String fileResultName = args[2];
		commitBatch = 4000;

		JanusG = JanusGraphFactory.open(confPath);

		ManagementSystem mgmt = (ManagementSystem) JanusG.openManagement();
		mgmt.makeEdgeLabel("MyEdge").make();
		mgmt.makeVertexLabel("MyNode").make();
		PropertyKey id_key = mgmt.makePropertyKey("id").dataType(String.class).make();

		mgmt.buildIndex("byId", JanusGraphVertex.class).addKey(id_key).unique().buildCompositeIndex();
		mgmt.commit();
//		try{
//			mgmt.awaitGraphIndexStatus(JanusG, "byId").call();
//		}
//		catch(Exception ex) {
//			ex.printStackTrace();
//			System.exit(-1);
//		}
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
