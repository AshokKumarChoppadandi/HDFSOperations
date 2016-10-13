package com.cloudwick.fileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;

/**
 * Created by cloudwick on 10/12/16.
 */
public class HdfsAllOperations {

    private static FileSystem fs;
    public HdfsAllOperations(Configuration conf) throws IOException {
        fs = FileSystem.get(conf);
    }

    public void readFile(Path path) throws IOException {
        if(!fs.exists(path)){
            System.out.println("No such file or Directory :: " + path);
        }

        FSDataInputStream fis = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line;
        while((line = br.readLine()) != null){
            System.out.println(line);
        }
    }

    public void createFile(Path path) throws IOException {

        if(!fs.exists(path)){
            Boolean bool = fs.createNewFile(path);
            if(bool){
                System.out.println("File created successfully at :: " + path);
            }
        } else {
            throw new FileAlreadyExistsException();
        }
    }

    public void deleteFile(Path path) throws IOException {
        if(fs.exists(path)){
            if(fs.delete(path, true)){
                System.out.println("File '" + path + "' Successfully deleted...!!!");
            }
        }
        else {
            throw new FileNotFoundException();
        }
    }

    public void writeFile(File sourceFile, Path destinationPath) throws IOException {
        FileInputStream fis = new FileInputStream(sourceFile);
        if(!fs.exists(destinationPath)){
            fs.createNewFile(destinationPath);
        }

        FSDataOutputStream fos = fs.append(destinationPath);
        OutputStream bw = new BufferedOutputStream(fos);
        byte[] b = new byte[1024];

        int numOfBytes = 0;
        while((numOfBytes = fis.read(b)) > 0){
            bw.write(b, 0, numOfBytes);
        }
        System.out.println("File Written to HDFS successfully");
        bw.close();
    }

    public void closeFileSystem() throws IOException {
        fs.close();
    }

}
