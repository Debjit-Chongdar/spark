package in.apache.spark.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import in.apache.spark.model.InputEmp;

public class ReadWriteUtil {

	private Dataset<Row> readCSV(String inputPath) {
		//if csv contains header thn specify that in option while reading
		return sparkSession.read().option("header", "true").csv(inputPath);
	}

	private Dataset<Row> readParquet(String inputPath) {
		return sparkSession.read().parquet(inputPath);
	}

	private Dataset<Row> readAvro(String inputPath) {
		return sparkSession.read().format("avro").load(inputPath);
	}

	private Dataset<Row> readJson(String inputPath) {
		return sparkSession.read().json(inputPath);
	}

	private Dataset<Row> readORC(String inputPath) {
		return sparkSession.read().orc(inputPath);
	}
	
	JavaRDD<String> readText(String inputPath) {
		return sparkSession.read().textFile(inputPath).javaRDD();
	}

	private void writeParquet(Dataset<Row> dataFrame, String outputPath, SaveMode mode, String partitionedByCol) {
		if(null == partitionedByCol){
			dataFrame.write().mode(mode).parquet(outputPath);
		}else{
			dataFrame.write().mode(mode).partitionBy(partitionedByCol).parquet(outputPath);
		}
	}

	private void writeJson(Dataset<Row> dataFrame, String outputPath, SaveMode mode, String partitionedByCol) {
		if(null == partitionedByCol){
		dataFrame.write().mode(mode).json(outputPath);
		}else{
			dataFrame.write().mode(mode).partitionBy(partitionedByCol).json(outputPath);
		}
	}

	private void writeCSV(Dataset<Row> dataFrame, String outputPath, SaveMode mode, String partitionedByCol) {
		//if specify header is true, thn it write csv with column name at header, otherwise it write only content
		if(null == partitionedByCol){
		dataFrame.write().option("header", "true").mode(mode).csv(outputPath);
		}else{
			dataFrame.write().option("header", "true").mode(mode).partitionBy(partitionedByCol).csv(outputPath);
		}
	}
	
	private void writeAvro(Dataset<Row> dataFrame, String outputPath, SaveMode mode, String partitionedByCol) {
		if(null == partitionedByCol){
		dataFrame.write().format("avro").mode(mode).save(outputPath);
		}else{
			dataFrame.write().format("avro").mode(mode).partitionBy(partitionedByCol).save(outputPath);
		}
	}

	private void save(Dataset<Row> dataFrame, String outputPath, SaveMode mode, String partitionedByCol) {
		if(null == partitionedByCol){
		dataFrame.write().mode(mode).save(outputPath);
		}else{
			dataFrame.write().mode(mode).partitionBy(partitionedByCol).save(outputPath);
		}
	}

	private void writeORC(Dataset<Row> dataFrame, String outputPath, SaveMode mode, String partitionedByCol) {
		if(null == partitionedByCol){
		dataFrame.write().mode(mode).orc(outputPath);
		}else{
			dataFrame.write().mode(mode).partitionBy(partitionedByCol).orc(outputPath);
		}
	}

	private Dataset<Row> readTextInput(String inputPath) {
		Dataset<String> stringDS = sparkSession.read().textFile(inputPath);
		stringDS.show();
		// JavaRDD<String> stringRDD = stringDS.javaRDD();
		// Encoder<InputEmp> encoder = Encoders.bean(InputEmp.class);
		// Dataset<InputEmp> empDS = stringDS.as(encoder); //it won't work
		// (Single string can not be converted to a bean class object)
		// empDS.show();
		Dataset<InputEmp> empDS = convertDatasetStringToDatasetInputEmp(stringDS);
		empDS.show();
		Dataset<Row> empDF = empDS.toDF();
		// Dataset<Row> empDF = sparkSession.createDataFrame(stringRDD,
		// InputEmp.class); //it won't work (Single string can not be converted
		// to a bean class object)
		empDF.show();
		return empDF;
	}

	private static Dataset<InputEmp> convertDatasetStringToDatasetInputEmp(Dataset<String> sourceDs) {
		Encoder<InputEmp> encoder = Encoders.bean(InputEmp.class);
		return sourceDs.map(new MapFunction<String, InputEmp>() {
			public InputEmp call(String inputLine) {
				String s[] = inputLine.split(",");
				InputEmp inputEmp = new InputEmp();
				inputEmp.setId(Long.parseLong(s[0]));
				inputEmp.setName(s[1]);
				inputEmp.setSal(Double.parseDouble(s[2]));
				inputEmp.setCountry(s[3]);
				inputEmp.setAge(Long.parseLong(s[4]));
				inputEmp.setRating(s[5]);
				inputEmp.setDept_id(Long.parseLong(s[6]));
				return inputEmp;
			}
		}, encoder);
	}
	
	public Dataset<Row> readFile(String inputPath){
		String inputType = inputPath.contains("csv")? "csv":inputPath.contains("parquet")?"parquet":
			inputPath.contains("json")?"json":inputPath.contains("avro")?"avro":
				inputPath.contains("orc")?"orc":inputPath.contains("text")?"text":"parquet";
		switch (inputType) {
		case "parquet":
			return this.readParquet(inputPath);
		case "csv":
			return this.readCSV(inputPath);
		case "json":
			return this.readJson(inputPath);
		case "avro":
			return this.readAvro(inputPath);
		case "orc":
			return this.readORC(inputPath);
		case "text":
			return this.readTextInput(inputPath);
		default:
			return this.readParquet(inputPath);
		}
	}
	
	public void writeFile(String writeType, Dataset<Row> dataFrame, String outputPath, SaveMode mode, String partitionedByCol) {
		switch (writeType) {
		case "avro":writeAvro(dataFrame, outputPath, mode, partitionedByCol);
		break;
		case "parquet":writeParquet(dataFrame, outputPath, mode, partitionedByCol);
		break;
		case "csv":writeCSV(dataFrame, outputPath, mode, partitionedByCol);
		break;
		case "orc":writeORC(dataFrame, outputPath, mode, partitionedByCol);
		break;
		case "json":writeJson(dataFrame, outputPath, mode, partitionedByCol);
		break;
		default:save(dataFrame, outputPath, mode, partitionedByCol);
		break;
		}
	}
	
	private SparkSession sparkSession;
	
	public ReadWriteUtil(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
	}
}
