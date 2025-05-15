package in.apache.spark.controller;

import org.apache.spark.sql.SparkSession;

import in.apache.spark.service.DatasetDataFrame;
import in.apache.spark.service.DatasetImpl;
import in.apache.spark.service.RddImpl;

public class AppController {

	public void process(SparkSession sparkSession, String inputPath, String outputPath, String implType) {
		if (implType.equalsIgnoreCase("Dataset")) {
			new DatasetImpl(sparkSession).process(inputPath, outputPath);
		}else if("Rdd".equalsIgnoreCase(implType)){
			new RddImpl(sparkSession).process(inputPath, outputPath);
		}else{
			new DatasetDataFrame(sparkSession).process(inputPath, outputPath);
		}
	}

}