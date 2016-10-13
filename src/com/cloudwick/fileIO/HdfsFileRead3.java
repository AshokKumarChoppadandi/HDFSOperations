package com.cloudwick.fileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by cloudwick on 10/12/16.
 */
public class HdfsFileRead3 {
    public static void main(String[] args) {
        String namenode = "hdfs://localhost:8020";
        String file = "hdfs://localhost:8020/user/cloudwick/WordCountInput";

        Configuration conf = new Configuration();
        //conf.set("fs.default.name", namenode);
        conf.set("fs.defaultFS", namenode);

        try{
            readFile(conf, file);
        } catch (IOException ioe){
            System.out.println();
        }


    }

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
