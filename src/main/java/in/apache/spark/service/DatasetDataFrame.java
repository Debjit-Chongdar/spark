package in.apache.spark.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import in.apache.spark.model.Dept;
import in.apache.spark.model.Emp;
import in.apache.spark.model.EmpDept;

public class DatasetDataFrame {

	private SparkSession sparkSession;
	
	public DatasetDataFrame(SparkSession sparkSession){
		this.sparkSession = sparkSession;
	}

	public void process(String inputPath, String outputPath){
		Dataset<Row> empDF = sparkSession.read().json(inputPath+"emp/");
		Dataset<Row> deptDF = sparkSession.read().json(inputPath+"dept/");
		Dataset<Row> sourceDF = empDF.join(deptDF, empDF.col("deptId").equalTo(deptDF.col("id")));
		//this.processDF(sourceDF, outputPath);
		this.processDS(empDF, deptDF, outputPath);
	}
	
	private void processDF(Dataset<Row> sourceDF, String outputPath){
		sourceDF.printSchema();
		sourceDF = sourceDF.filter(sourceDF.col("deptName").equalTo(functions.lit("IT").cast(DataTypes.StringType)));
		sourceDF = sourceDF.orderBy(sourceDF.col("salary"), sourceDF.col("address.city")).select("name","deptName","salary","address.city");
		sourceDF.write().format("avro").save(outputPath+"/avro/");
		Dataset<Row> dataset = sparkSession.read().format("avro").load(outputPath+"/avro/");
		dataset.show();
	}
	
	
	private void processDS(Dataset<Row> empDF, Dataset<Row> deptDF, String outputPath){
		Encoder<Emp> empEncoder = Encoders.bean(Emp.class);
		Encoder<Dept> deptEncoder = Encoders.bean(Dept.class);
		Encoder<EmpDept> empDeptEncoder = Encoders.bean(EmpDept.class);
		
		Dataset<Emp> empDS = empDF.as(empEncoder);
		Dataset<Dept> deptDS = deptDF.as(deptEncoder);
		empDS.show();
		deptDS.show();
		
		Dataset<EmpDept> empDeptDS = empDS.join(deptDS, empDS.col("deptId").equalTo(deptDS.col("id")))
				.select(empDS.col("id"),empDS.col("name"),empDS.col("salary"),empDS.col("deptId"),empDS.col("address"),deptDS.col("deptName"))
				.as(empDeptEncoder);
		empDeptDS.show();
	}
}