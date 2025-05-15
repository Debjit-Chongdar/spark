package in.apache.spark.controller;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AppControllerTest {

	private static SparkSession sparkSession;
	private static AppController controller;
	
	private String SOURCE_DIR = "/DataStore/Dataset/avro/";
	//private String SOURCE_DIR = "/DataStore/Dataset/parquet/";
	//private String SOURCE_DIR = "/DataStore/Dataset/orc/";
	//private String SOURCE_DIR = "/DataStore/Dataset/json/";
	//private String SOURCE_DIR = "/DataStore/Dataset/text/";
	//private String SOURCE_DIR = "/DataStore/Dataset/csv/";
	private String TARGET_DIR = "TargetDataStore/TargetDataSet/";
	
	@BeforeClass
	public static void setUp() {
		controller = new AppController();
		
		String hadoopHomePath = AppControllerTest.class.getResource("/hadoop-2.6.5/").getPath();
		System.setProperty("hadoop.home.dir", hadoopHomePath);
		
		sparkSession = SparkSession
				.builder()
				.appName("App Test")
				.master("local[*]")
				.config("spark.testing.memory", "2147480000")
				.getOrCreate();
	}

	//@Test
	public void testProcessDataset() {
		String inputPath = AppControllerTest.class.getResource(SOURCE_DIR).getPath();
		String outputPath = AppControllerTest.class.getResource("/").getPath()+TARGET_DIR;
		controller.process(sparkSession, inputPath, outputPath,"Dataset");
	}
	
	//@Test
	public void testProcessRDD() {
		controller.process(sparkSession, AppControllerTest.class.getResource("/source/").getPath(),
				AppControllerTest.class.getResource("/").getPath()+"target/","Rdd");
	}
	
	@Test
	public void testProcessDatasetDataFrame() {
		controller.process(sparkSession, AppControllerTest.class.getResource("/DataStore/Dataset/DsDFSource/").getPath(),
				AppControllerTest.class.getResource("/").getPath()+"target/DsDFSource/","DatasetDataFrame");
	}
	
	@AfterClass
	public static void tearDown() {
		sparkSession.close();
		sparkSession.stop();
	}
}
