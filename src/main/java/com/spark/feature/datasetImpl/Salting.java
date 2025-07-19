package com.spark.feature.datasetImpl;

import com.spark.feature.util.bean.Dept;
import com.spark.feature.util.bean.Emp;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class Salting {
    private SparkSession session =
            SparkSession.builder().appName("Salting Dataset").master("local[*]").getOrCreate();

    public void salting_Dataset_join() {
        session.sparkContext().setLogLevel("ERROR");
        Dataset<Row> largeDataset = session.createDataFrame(
                Arrays.asList(
                        new Emp("Ind", "abc", 1),
                        new Emp("Fr", "bcd", 2),
                        new Emp("Aus", "efg", 2),
                        new Emp("Swz", "cde", 1),
                        new Emp("Amer", "def", 1)
                ), Emp.class);
        Dataset<Row> smallDataset = session.createDataset(
                Arrays.asList(
                        new Dept(1, "Eng"),
                        new Dept(2, "Hr")
                ), Encoders.bean(Dept.class)).toDF();
        // use random number lesser than the minimum occurance in both side
        int saltRange = 2;
        Dataset<Row> saltedLargeDataset = largeDataset.withColumn("salted_dept_id",
                concat(expr("CAST(FLOOR(RAND() * " + saltRange + ") AS INT)"), concat(lit("_"), largeDataset.col("dept_id"))));

        Dataset<Row> expandedSmallDataset = smallDataset.flatMap((FlatMapFunction<Row, Row>) row -> {
                    List<Row> rows = new ArrayList<>();
                    for (int i = 0; i < saltRange; i++) {
                        rows.add(RowFactory.create(i+"_"+row.getInt(0), row.getInt(0), row.getString(1)));
                    }
                    return rows.iterator();
                }, RowEncoder.apply(new StructType()
                        .add("salted_id", DataTypes.StringType)
                        .add("id", DataTypes.IntegerType)
                        .add("name", DataTypes.StringType)
                )
        );
        saltedLargeDataset.show();
        expandedSmallDataset.show();
        Dataset<Row> joinedDataset = saltedLargeDataset.join(expandedSmallDataset,
                saltedLargeDataset.col("salted_dept_id").equalTo(expandedSmallDataset.col("salted_id")), "leftouter");
        joinedDataset = joinedDataset.drop("salted_dept_id", "salted_id");
        joinedDataset.show();
        //new Scanner(System.in).nextLine();
    }

    // salting large dataset key
    // expand small dataset key
    // join both
    public void salting_RDD_join() {
        session.sparkContext().setLogLevel("ERROR");
        JavaPairRDD<String, String> largeRDD = JavaSparkContext.fromSparkContext(session.sparkContext())
                .parallelizePairs(Arrays.asList(
                        new Tuple2<>("a", "abc"),
                        new Tuple2<>("a", "ac"),
                        new Tuple2<>("a", "acb"),
                        new Tuple2<>("b", "bca"),
                        new Tuple2<>("b", "bac")
                ));
        JavaPairRDD<String, String> smallRDD = JavaSparkContext.fromSparkContext(session.sparkContext())
                .parallelizePairs(Arrays.asList(
                        new Tuple2<>("a", "aa"),
                        new Tuple2<>("b", "bb")
                ));
        // apply salting in large dataset
        int saltRange = 2;
        JavaPairRDD<String, String> saltedLargeRDD = largeRDD.mapToPair(tuple2 -> {
            Random rand = new Random();
            int salt = rand.nextInt(saltRange);
            return new Tuple2<>(tuple2._1() + salt, tuple2._2());
        });
        //expand small dataset to properly join with salted large dataset
        JavaPairRDD<String, String> expandedSmallRDD = smallRDD.flatMapToPair(tuple2 -> {
            List<Tuple2<String, String>> list = new ArrayList<>();
            for (int i = 0; i < saltRange; i++) {
                list.add(new Tuple2<>(tuple2._1() + i, tuple2._2()));
            }
            return list.iterator();
        });
        JavaPairRDD<String, Tuple2<String, Optional<String>>> joinedRDD = saltedLargeRDD.leftOuterJoin(expandedSmallRDD);
        // show data afer join
        joinedRDD.foreach(tuple2 ->
                System.out.println(tuple2._1() + "  " + tuple2._2()._1() + "  " + tuple2._2()._2())
        );
    }

    public void salting_GroupBy(){
        session.sparkContext().setLogLevel("ERROR");
        JavaPairRDD<String, Integer> pairRDD = JavaSparkContext.fromSparkContext(session.sparkContext())
                .parallelizePairs(Arrays.asList(
                        new Tuple2<>(1, "abc"),
                        new Tuple2<>(1, "bcd"),
                        new Tuple2<>(1, "def"),
                        new Tuple2<>(2, "xyz")
                )).mapToPair(pair -> {
                    return new Tuple2<>(pair._1()+"_"+new Random().nextInt(2), 1);
                });
        pairRDD = pairRDD.reduceByKey((a,b) -> a+b) // reduce with salted key
                .mapToPair(tuple2 -> { // here we remove salted append part
                    return new Tuple2<>(tuple2._1().split("_")[0], tuple2._2());
                }).reduceByKey((a,b) -> a+b); //re reduce without salted part
        pairRDD.foreach(tuple2 -> System.out.println(tuple2._1()+" => Occurance => "+tuple2._2()));
    }
    //Salting can use repartition, window function, aggregation
    // dataset.repartition(col("salted_key"));
    // .over(Window.partitionBy("salted_key")) then re aggregate

    public static void main(String[] args) {
        new Salting().salting_Dataset_join();
        new Salting().salting_RDD_join();
        new Salting().salting_GroupBy();
    }
}
