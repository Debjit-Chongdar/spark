package in.apache.spark.SparkTest;

import java.io.File;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import in.apache.spark.controller.AppController;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	if(args.length<2){
        	System.out.println("Please provide both input and output path");
        	System.exit(1);
        }
        SparkSession sparkSession = SparkSession
        		.builder()
        		.appName("Word Count App")
        		//To work with hive add below commented config along with hive-site.xml, core-site.xml(for security configuration), and hdfs-site.xml
        		//.config("spark.sql.warehouse.dir", new File("spark-warehouse").getAbsolutePath())
        		//.enableHiveSupport()
        		.getOrCreate();
        new AppController().process(sparkSession, args[0], args[1],args[2]);

        sparkSession.close();
        sparkSession.stop();
    }
}
