package com.cloudwick.fileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Created by AshokKumarChoppadandi on 9/23/16.
 */
public class HdfsFileRead
{
    /**
     * Default Constructor
     */
    public HdfsFileRead() {

    }

    /**
     * @param file - HDFS File to read
     * @param conf - HDFS Configuration
     * @throws IOException - Throws Exception if the File System not found.
     */
    public void readFile(String file, Configuration conf) throws IOException
    {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(file);
        if(!fs.exists(path))
        {
            System.out.println("File " + file + "Not Found");
            return;
        }
        FSDataInputStream fsIn = fs.open(path);
        //String filename = file.substring(file.lastIndexOf("/") + 1, file.length());
        //OutputStream out = new BufferedOutputStream(new OutputStream());
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(fsIn));
        while((line = br.readLine()) != null)
        {
            System.out.println("Text :: " + line);
        }
        /*
        byte[] b = new byte[1024];
        int numBytes = 0;
        String res = fsIn.readUTF();
        System.out.println("Text :: " + res);
        while((numBytes = fsIn.read(b)) > 0)
        {
            //out.write(b, 0, numBytes);
            System.out.println("Content" + b.toString());
        }
        out.close();
        */
        fsIn.close();
        br.close();
        fs.close();
    }

    /**
     * @param args - Command Line Arguments
     * @throws IOException - Throws Exception if the any problem occurs in calling 'readFile' Method
     */
    public static void main(String[] args) throws IOException
    {
        HdfsFileRead hfs = new HdfsFileRead();
        String hdfsPath = "hdfs://localhost:8020";
        String filePath = "hdfs://localhost:8020/user/cloudwick/WordCountInput";
        Configuration conf = new Configuration();
        conf.set("fs.default.name", hdfsPath);
        hfs.readFile(filePath, conf);
    }
}
