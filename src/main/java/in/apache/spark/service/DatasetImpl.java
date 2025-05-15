package in.apache.spark.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;

import in.apache.spark.model.PromotedEmp;
import in.apache.spark.util.ReadWriteUtil;

public class DatasetImpl {

	private SparkSession sparkSession;
	private ReadWriteUtil readWriteUtil;

	public DatasetImpl(SparkSession session) {
		this.sparkSession=session;
		this.readWriteUtil = new ReadWriteUtil(sparkSession);
	}
	public void process(String inputPath, String outputPath) {
		Dataset<Row> sourceDS = readWriteUtil.readFile(inputPath);
		String inputFileType = inputPath.contains("csv")? "csv":inputPath.contains("parquet")?"parquet":
			inputPath.contains("json")?"json":inputPath.contains("avro")?"avro":
				inputPath.contains("orc")?"orc":inputPath.contains("text")?"txt":null;
		this.workWithSQL(sourceDS, outputPath,inputFileType);
		Dataset<Row> joinedDS= this.performJoin(outputPath, sourceDS, inputFileType);
		this.filterGroupBy(joinedDS, outputPath, inputFileType);
		this.savePromotedEmp(outputPath, inputFileType);
		Dataset<PromotedEmp> dataset = this.convertDatasetRowToDatasetBean(outputPath, inputFileType);
		DatasetImpl.functionsOfDataset(dataset);
	}

	private void workWithSQL(Dataset<Row> sourceDS, String outputPath, String inputFileType) {
		Dataset<Row> filteredDS = null;
		try {
			sourceDS.createTempView("Temp");//it's specific to session
			sourceDS.createOrReplaceTempView("Temp");
			filteredDS = sparkSession.sql("SELECT t.* FROM Temp AS t WHERE t.sal>6000");
			
			filteredDS.cache(); //Both are default storage == MEMORY_AND_DISK
			filteredDS.persist();//Both are default storage == MEMORY_AND_DISK
			filteredDS.persist(StorageLevels.NONE); 	//not persisted
			filteredDS.persist(StorageLevels.MEMORY_ONLY); //Store as deserialized in the JVM. If it does not fit in memory,some partitions will not be cached
			filteredDS.persist(StorageLevels.MEMORY_ONLY_SER);	//Store as serialized in the JVM. more space efficient thn deserialized object
			filteredDS.persist(StorageLevels.MEMORY_ONLY_2);	//Store the Dataset partitions only on disk.but replicate each partition on two cluster nodes.
			filteredDS.persist(StorageLevels.MEMORY_ONLY_SER_2);
			filteredDS.persist(StorageLevels.MEMORY_AND_DISK);
			filteredDS.persist(StorageLevels.MEMORY_AND_DISK_2); //Store the Dataset partitions only on disk.but replicate each partition on two cluster nodes.
			filteredDS.persist(StorageLevels.MEMORY_AND_DISK_SER);
			filteredDS.persist(StorageLevels.MEMORY_AND_DISK_SER_2);
			filteredDS.persist(StorageLevels.DISK_ONLY);	//Store the Dataset partitions only on disk.
			filteredDS.persist(StorageLevels.DISK_ONLY_2);
			filteredDS.persist(StorageLevels.OFF_HEAP); //Similar to MEMORY_ONLY_SER, but store the data in off-heap memory.This requires off-heap memory to be enabled.
			
			filteredDS.createOrReplaceGlobalTempView("Associates");//it's not bound with session
			//filteredDS.createGlobalTempView("Associates");
			// Global temporary view is tied to a system preserved database `global_temp`
			filteredDS = sparkSession.newSession().sql("SELECT name, sal FROM global_temp.Associates");//newSession only work with GlobalTemp			
			
			readWriteUtil.writeFile(inputFileType, filteredDS, outputPath+inputFileType+"/intermidiate/", SaveMode.Append, null);
		} catch (AnalysisException e) {
			e.printStackTrace();
		}finally{
			if(filteredDS != null){
				filteredDS.unpersist();
			}
		}
	}
	
	private Dataset<Row> performJoin(String outputPath, Dataset<Row> sourceDS,String inputFileType) {
		//read files saved by upper method
		Dataset<Row> storedDF = readWriteUtil.readFile(outputPath+inputFileType+"/intermidiate/");
		
		Dataset<Row> joinedDS = storedDF.join(sourceDS,	//leftouter join based on name and sal
				storedDF.col("name").equalTo(sourceDS.col("name")).and(storedDF.col("sal").equalTo(sourceDS.col("sal")))
				,"leftouter").select(sourceDS.col("*"));	// selecting sourceDS all fields
		joinedDS = joinedDS.orderBy(joinedDS.col("name"));
		joinedDS = joinedDS.withColumnRenamed("sal","salary");	//rename column sal to salary
		joinedDS = joinedDS.withColumn("variable", joinedDS.col("salary").cast(DataTypes.DoubleType)	//adding field variable
						.multiply(functions.lit(12)).divide(functions.lit(100)));	// 12 percent of salary is variable
		
		return joinedDS;
	}
	
	private void filterGroupBy(Dataset<Row> storedDF, String outputPath, String inputFileType){
		storedDF.show();
		storedDF.persist(StorageLevel.MEMORY_ONLY_SER());
		
		Dataset<Row> filteredLeadDS = storedDF.where(storedDF.col("age").cast(DataTypes.LongType).geq(28).and(storedDF.col("rating").isin("Excelent","Very Good")));
		Dataset<Row> leadDS = filteredLeadDS.withColumn("job_role", functions.lit("Lead").cast(DataTypes.StringType)) //adding job_role column as constant value Lead
				.withColumn("emp_band", functions.lit(9).cast(DataTypes.IntegerType));	//adding emp_band column as constant value 9
		
		// write into disk
		readWriteUtil.writeFile(inputFileType, leadDS, outputPath+inputFileType+"/band_9/", SaveMode.Overwrite,"dept_id");
		
		Dataset<Row> groupLeadDS = leadDS.groupBy(leadDS.col("dept_id"))	//grouping dept wise
				.agg(functions.avg(leadDS.col("variable")),functions.count(leadDS.col("salary")));
		groupLeadDS.show();
		
		Dataset<Row> sseDF = storedDF.filter(storedDF.col("age").cast(DataTypes.LongType).geq(27)	//cast age into long and verify if age>=27
				.and(storedDF.col("rating").isin("Excelent","Very Good","Good")));
		sseDF = sseDF.except(filteredLeadDS);
		sseDF = sseDF.withColumn("job_role", functions.lit("SSE").cast(DataTypes.StringType))
				.withColumn("emp_band", functions.lit(10).cast(DataTypes.IntegerType));	//cast 10 into integer
		sseDF.show();
		
		readWriteUtil.writeFile(inputFileType, sseDF, outputPath+inputFileType+"/band_10/", SaveMode.Overwrite,"dept_id");
		
		sseDF = sseDF.groupBy(sseDF.col("dept_id")).count();	//dept wise count
		sseDF.show();
		storedDF.unpersist();
	}
	
	private void savePromotedEmp(String outputPath,String inputFileType){
		Dataset<Row> band10DS = readWriteUtil.readFile(outputPath+inputFileType+"/band_10/");
		Dataset<Row> band9DS = readWriteUtil.readFile(outputPath+inputFileType+"/band_9/");
		band10DS.show();
		band9DS.show();
		Dataset<Row> promotedDS = band10DS.union(band9DS);	//combine both band 9 and band 10 promoted employees
		promotedDS = promotedDS.orderBy("dept_id").select("dept_id","id","emp_band","job_role","name","salary").distinct();
		
		//Write on disk as JSON
		readWriteUtil.writeFile(inputFileType, promotedDS, outputPath+inputFileType+"/target/", SaveMode.Append,null);
	}
	
	private Dataset<PromotedEmp> convertDatasetRowToDatasetBean(String outputPath,String inputFileType){
		Dataset<Row> promotedEmp = readWriteUtil.readFile(outputPath+inputFileType+"/target/");
		promotedEmp.show();
		Encoder<PromotedEmp> encoder = Encoders.bean(PromotedEmp.class); //encoder is used to convert Row to specific bean
	//	Encoder<Tuple2<T1,T2>> tuple2Encoder = Encoders.tuple(Encoders.bean(T1.class), Encoders.bean(T2.class));
	//	Encoder<Tuple3<T1,T2,T3>> tuple3Encoder = Encoders.tuple(Encoders.bean(T1.class), Encoders.bean(T2.class),Encoders.bean(T3.class));
	//	Encoder<Tuple4<T1,T2,T3,T4>> tuple4Encoder = Encoders.tuple(Encoders.bean(T1.class),..,Encoders.bean(T4.class));
	//	Encoder<Tuple5<T1,T2,T3,T4,T5>> tuple5Encoder = Encoders.tuple(Encoders.bean(T1.class),...., Encoders.bean(T5.class));
		
		Dataset<PromotedEmp> dataset = promotedEmp.groupBy(promotedEmp.col("dept_id").as("deptId")	//change column name as per mentioned in bean class
				,promotedEmp.col("emp_band").as("empBand"))
				.agg(functions.avg(promotedEmp.col("salary")).as("avgSalary"),	//change column name as per mentioned in bean class
						functions.max(promotedEmp.col("salary")).as("maxSalary"))
				.as(encoder).limit(20); //return max 20 PromotedEmp
		dataset.show();
		return dataset;
	}
	
	private static void functionsOfDataset(Dataset<PromotedEmp> dataset){
		Dataset<Row> dataFrame = dataset.toDF();
		dataFrame.show();
		Dataset<Row> renamedDataFrame = dataset.toDF("dept_id","emp_band","avg_salary","max_salary")
				.drop("dept_id") 	//drop one or multiple column
				.dropDuplicates(); 	//Returns a new Dataset that contains only the unique rows from this Dataset.
		renamedDataFrame.show();
	//	Dataset<String> json = dataset.toJSON();	//throw exception
	//	json.show();	
		
		RDD<PromotedEmp> rdd = dataset.rdd();
		JavaRDD<PromotedEmp> javaRdd = dataset.javaRDD();
		JavaRDD<PromotedEmp> toJavaRdd = dataset.toJavaRDD();
	//	List<PromotedEmp> rows = dataset.collectAsList(); // exception "PromotedEmp.setDeptId(int) for actual parameters "long""
	//	List<PromotedEmp> fewRows = dataset.takeAsList(5); //return top 5 PromotedEmp from dataset
		SQLContext sqlContext = dataset.sqlContext();
		Dataset<PromotedEmp> sortedDataset = dataset.sort(dataset.col("deptId"));
		sortedDataset.show();
		//Dataset<PromotedEmp> sortedDataset1 = dataset.sortWithinPartitions(dataset.col("dept_id"));
		Dataset<PromotedEmp> promotedEMP1 = dataFrame.map(new MapFunction<Row, PromotedEmp>() {
			public PromotedEmp call(Row row){
				PromotedEmp emp = new PromotedEmp();
				emp.setDeptId(Integer.parseInt(row.getAs("deptId").toString()));	//this field became long after convert to dataFrame
				emp.setEmpBand(Integer.parseInt(row.getAs("empBand").toString()));	//this field became long after convert to dataFrame
				emp.setAvgSalary(row.getAs("avgSalary")); 
				emp.setMaxSalary(Double.parseDouble(row.getAs("maxSalary").toString()));	//this field became String after convert to dataFrame
				return emp;
			}
		}, Encoders.bean(PromotedEmp.class));
		promotedEMP1.show();	
		//Dataset<PromotedEmp> promotedEMP2 = promotedEmp.flatMap(new FlatMapFunction<Row, PromotedEmp>(){...}, encoder);
	}
}
