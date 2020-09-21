package com.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by AshokKumarChoppadandi on 10/12/16.
 */
public class HdfsFileRead3 {
    /**
     * @param args - Command line Arguments
     */
    public static void main(String[] args) {
        //String nameNode = "hdfs://localhost:8020";
        String nameNode = args[0];
        String file = args[1];
        Configuration conf = new Configuration();
        //conf.set("fs.default.name", nameNode);
        conf.set("fs.defaultFS", nameNode);
        try{
            readFile(conf, file);
        } catch (IOException ioe){
            System.out.println();
        }
    }

    /**
     * @param conf - HDFS Configuration
     * @param file - HDFS File path to read
     * @throws IOException - Throws Exception if the file not found in HDFS
     */
    private static void readFile(Configuration conf, String file) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path filePath = new Path(file);
        if(!fs.exists(filePath)){
            System.out.println("No such file or Directory at :: " + filePath);
        }
        FSDataInputStream fis = fs.open(filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line;
        while((line = br.readLine()) != null){
            System.out.println(line);
        }
    }
}
