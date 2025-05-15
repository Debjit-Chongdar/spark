package in.apache.spark.service;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.ListIterator;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class RddImpl implements Serializable{

	private transient SparkSession sparkSession;
	private transient SparkContext sc;
	private transient JavaSparkContext jsc;
	private transient SQLContext sqlContext;

	public RddImpl(SparkSession sparkSession) {
		this.sparkSession = sparkSession;
		this.sc = sparkSession.sparkContext();
		this.jsc = new JavaSparkContext(this.sc);
		this.sqlContext = new SQLContext(jsc);
		this.sqlContext = new SQLContext(sparkSession);
		this.sqlContext = sparkSession.sqlContext();
	}

	public void process(String inputPath, String outputPath) {
		JavaRDD<String> javaRDD = jsc.textFile(inputPath);
		RDD<String> rdd = sc.textFile(inputPath, 0);
		Dataset<String> dataset = sparkSession.read().textFile(inputPath);
		Dataset<String> dataset1 = sqlContext.read().textFile(inputPath);
		Dataset<Row> dataFrame = sparkSession.read().text(inputPath);
		Dataset<Row> dataFrame1 = sqlContext.read().text(inputPath);

		processJavaRDD(javaRDD, outputPath);
		processRDD(rdd, outputPath);
		processDatasetStrSession(dataset, outputPath);
		processDatasetStrSql(dataset1, outputPath);
		processDatasetRowSession(dataFrame, outputPath);
		processDatasetRowSql(dataFrame1, outputPath);
	}

	/**
	 * Description : Occurrence of each word
	 * 
	 * @param javaRDD
	 * @param outputPath
	 */
	public void processJavaRDD(JavaRDD<String> javaRDD, String outputPath) {
		JavaRDD<String> flatMapRdd = javaRDD.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String line) {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});
		JavaRDD<String> flatMapRdd_1 = javaRDD.flatMap(str -> Arrays.asList(str.split("")).iterator());
		JavaPairRDD<String, Integer> pairRDD = flatMapRdd.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String word) {
				return new Tuple2(word, 1);
			}
		});
		pairRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) {
				return v1 + v2;
			}
		});
		pairRDD.saveAsTextFile(outputPath + "/JavaRDD/");
	}

	/**
	 * Description : Occurrence of same IP
	 * 
	 * @param rdd
	 * @param outputPath
	 */
	public void processRDD(RDD<String> rdd, String outputPath) {
		rdd.saveAsObjectFile(outputPath+"/rdd/");
	}

	/**
	 * Description : Occurrence of same IP
	 * 
	 * @param dataset
	 * @param outputPath
	 */
	public void processDatasetStrSession(Dataset<String> dataset, String outputPath) {
		JavaPairRDD<String, Integer> pairRDD = dataset.javaRDD().mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String line) {
				return new Tuple2(line.split("-")[0].trim(), 1);
			}
		});
		pairRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer v1, Integer v2) {
				return v1 + v2;
			}
		});
		pairRDD.saveAsTextFile(outputPath + "/datasetStrSession/");
	}

	public void processDatasetStrSql(Dataset<String> dataset, String outputPath) {
		JavaRDD<String> readJavaRDD = jsc.textFile(outputPath + "/datasetStrSession/");
		RDD<String> readRDD = sc.textFile(outputPath + "/datasetStrSession/", 1);
		Dataset<String> readDataset = sparkSession.createDataset(readRDD, Encoders.bean(String.class));
		Dataset<String> readSQL = sqlContext.createDataset(readRDD, Encoders.bean(String.class));
		
		dataset.write().mode(SaveMode.Append).save(outputPath+"/datasetStrSql/");
	}

	public void processDatasetRowSession(Dataset<Row> dataset, String outputPath) {
		dataset.write().mode(SaveMode.Ignore).format("avro").save(outputPath+"/datasetRowSession/");
	}

	public void processDatasetRowSql(Dataset<Row> dataset, String outputPath) {
		dataset.write().option("header", "true").mode(SaveMode.ErrorIfExists).csv(outputPath+"/datasetRowSql/");
	}

	private void methods(){
		JavaRDD<String> javaRDD_line = jsc.textFile("log.txt");
		JavaPairRDD<String, Integer> javaPairRDD_line = javaRDD_line.mapToPair(line -> new Tuple2<>(line, 1));
		JavaPairRDD<String, Integer> javaPairRDD_line_1 = javaRDD_line.mapToPair(new PairFunction<String, String, Integer>(){
			@Override
			public Tuple2<String, Integer> call(String line){
				return new Tuple2<>(line, 1);
			}
		});
		JavaPairRDD<String, Integer> pairRDD_ReduceByKey = javaPairRDD_line.reduceByKey((val1, val2) -> val1+val2 );
		JavaPairRDD<String, Integer> pairRDD_ReduceByKey_1 = javaPairRDD_line.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer integer, Integer integer2) throws Exception {
				return integer+integer2;
			}
		});

	}
}
