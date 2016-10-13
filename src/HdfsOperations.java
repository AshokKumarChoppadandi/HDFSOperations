
import com.cloudwick.fileIO.HdfsAllOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.Scanner;

/**
 * Created by AshokKumarChoppadandi on 10/12/16.
 */

public class HdfsOperations extends Configured implements Tool{
    /**
     * @param strings - Run Time Arguments
     * @return - An integer value, to identify the Execution.
     * @throws Exception - Throws Exception if File System not found
     */
    @Override
    public int run(String[] strings) throws Exception {
        if(strings.length > 0){
            System.err.println("HdfsOperations doesn't any arguments");
            return 1;
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:8020");
        conf.set("dfs.replication", "1");
        HdfsAllOperations h_ops = new HdfsAllOperations(conf);

        System.out.println("************** Welcome to Hadoop Distributed File System **************");
        System.out.println("Please Select your choice from the below options :: ");
        System.out.println("1. Create a New File.");
        System.out.println("2. Read a File.");
        System.out.println("3. Delete the File.");
        System.out.println("4. Write a File.");
        System.out.print("\nEnter your choice :: ");

        Scanner sc = new Scanner(System.in);
        int input = sc.nextInt();
        String file;
        Path path;

        switch (input){
            case 1 : System.out.print("Please enter the file name to create :: ");
                     file = sc.next();
                     path = new Path(conf.get("fs.defaultFS") + "/user/cloudwick/" + file);
                     h_ops.createFile(path);
                     break;
            case 2 : System.out.println("Please enter the file name to read :: ");
                     file = sc.next();
                     path = new Path(conf.get("fs.defaultFS") + "/user/cloudwick/" + file);
                     h_ops.readFile(path);
                     break;
            case 3 : System.out.println("Please enter the file name to delete :: ");
                     file = sc.next();
                     path = new Path(conf.get("fs.defaultFS") + "/user/cloudwick/" + file);
                     h_ops.deleteFile(path);
                     break;
            case 4 : System.out.println("Please enter the Source File Location :: ");
                     file = sc.next();
                     File f = new File(file);
                     System.out.println("Please enter the Destination File Name :: ");
                     file = sc.next();
                     path = new Path(conf.get("fs.defaultFS") + "/user/cloudwick/" + file);
                     h_ops.writeFile(f, path);
                     break;
            default : System.out.println("You Have entered an Invalid Choice.\n" +
                      "Please Enter the Correct choice...!!!");
                      break;
        }
        h_ops.closeFileSystem();
        return 0;
    }

    /**
     * @param args - Command Line Arguments
     * @throws Exception - Throws Exception in calling the run Method.
     */
    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new HdfsOperations(), args);
        System.exit(returnCode);
    }
}
