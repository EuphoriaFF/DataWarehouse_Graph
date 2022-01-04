package com.hlrspace.gremlin.presto;



import java.io.*;
import java.sql.*;

public class Main {

        static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
        static final String DB_URL = "jdbc:mysql://10.176.40.85:9030/graph500";
        static final String MY_DB_URL = "jdbc:mysql://localhost:3306/graph500";
        static final String USER = "test";
        static final String MY_USER = "root";
        static final String PASS = "123456";
        public static void main(String[] args) {
            Connection conn = null;
            Statement stmt ;
            try {
                Class.forName(JDBC_DRIVER);
                conn = DriverManager.getConnection(DB_URL, USER, PASS);
                //查询一度的语句
                PreparedStatement oneDegreePst = conn.prepareStatement("SELECT end_id FROM graph500.outEdges WHERE start_id = (?)");
                //查询二度的语句
                PreparedStatement twoDegreePst = conn.prepareStatement("SELECT end_id FROM graph500.outEdges WHERE start_id IN " +
                        "(SELECT end_id FROM graph500.outEdges WHERE start_id = (?))");
                //查询三度的语句
                PreparedStatement threeDegreePst = conn.prepareStatement(
                        "SELECT end_id FROM graph500.outEdges WHERE start_id IN" +
                        "(SELECT end_id FROM graph500.outEdges WHERE start_id IN " +
                        "(SELECT end_id FROM graph500.outEdges WHERE start_id = (?)))"
                );

                String path = "/Users/hlr/inventory/dev/work/fudan/datawarehouse_plus_graph/experiment/vertex_id_list.txt";
                File inputFile = new File(path);
                File outputFile = new File(inputFile.getParent() + "/out");
                outputFile.createNewFile();
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile));
                FileInputStream fileInputStream = new FileInputStream(inputFile);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));

                long currentTime, endTime;
                long oneRes, twoRes, threeRes;
                String str = null;
                while((str = bufferedReader.readLine()) != null) {
                    oneDegreePst.setString(1, str);
                    twoDegreePst.setString(1, str);
                    threeDegreePst.setString(1, str);

                    //一度结果
                    currentTime = System.currentTimeMillis();
                    oneDegreePst.executeQuery();
                    endTime = System.currentTimeMillis();
                    oneRes = endTime - currentTime;
                    //二度结果
                    currentTime = System.currentTimeMillis();
                    twoDegreePst.executeQuery();
                    endTime = System.currentTimeMillis();
                    twoRes = endTime - currentTime;

                    //三度结果
                    currentTime = System.currentTimeMillis();
                    threeDegreePst.executeQuery();
                    endTime = System.currentTimeMillis();
                    threeRes= endTime - currentTime;

                    String tmp = str+"\t"+oneRes+"\t"+twoRes+"\t"+threeRes+"\n";
                    System.out.print(tmp);
                    bufferedWriter.write(tmp);
                }

                bufferedReader.close();
                bufferedWriter.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
}
