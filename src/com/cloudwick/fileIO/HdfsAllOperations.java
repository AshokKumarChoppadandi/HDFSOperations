package com.cloudwick.fileIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;

/**
 * Created by AshokKumarChoppadandi on 10/12/16.
 */
public class HdfsAllOperations {

    /**
    * Initializing the FileSystem
    */
    private static FileSystem fs;

    /**
     * @param conf - HDFS Configuration
     * @throws IOException - Throws Exception if the FileSystem not found with the given configuration
     */
    public HdfsAllOperations(Configuration conf) throws IOException {
        fs = FileSystem.get(conf);
    }

    /**
     * @param path - HDFS Path to read the File
     * @throws IOException - Throws Exception if the File not found in the given path
     */
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

    /**
     * @param path - HDFS Path to Create the File
     * @throws IOException - Throws Exception if the File already exists in the given path
     */
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

    /**
     * @param path - HDFS Path to Create the File
     * @throws IOException - Throws Exception if the File not found in the given path
     */
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

    /**
     * @param sourceFile - Local File Location to be copied to HDFS
     * @param destinationPath - HDFS Path to copy the File
     * @throws IOException - Throws Exception if the File not found in the given path or
     *                       During writing the data to the file in HDFS.
     */
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

    /**
     * Closing the File System Connection
     * @throws IOException
     */
    public void closeFileSystem() throws IOException {
        fs.close();
    }

}
