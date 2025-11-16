package com.spark.feature.salting;

import com.spark.feature.salting.bean.Dept;
import com.spark.feature.salting.bean.Emp;
import com.spark.feature.salting.bean.RandomUdf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class Salting {
    private SparkSession session;

    public Salting(SparkSession session){
        this.session = session;
    }

    public void saltingOnDs(String empPath, String deptPath){
        Dataset<Row> empDs = session.read().json(empPath);
        Dataset<Row> deptDs = session.read().json(deptPath);
        Dataset<Long> saltDs = session.range(5);
        // cross join with dept as dept has less rows than empl
        Dataset<Row> saltedDeptDs = deptDs.crossJoin(saltDs).withColumn("salted_id",
                concat(deptDs.col("id"), functions.lit("_"), saltDs.col("id")));
        //add random value of range 5 to dept_id in empl
        session.udf().register("rand", new RandomUdf(), DataTypes.IntegerType);
        Dataset<Row> saltedEmpDs = empDs.orderBy("deptId").withColumn("salted_dept_id",
                concat(empDs.col("deptID"), functions.lit("_"), functions.callUDF("rand")));

        Dataset<Row> joinedDS = saltedDeptDs.join(saltedEmpDs,
                saltedDeptDs.col("salted_id").equalTo(saltedEmpDs.col("salted_dept_id")))
                .drop("salted_dept_id","salted_id")
                .select("name", "deptName","salary");
        joinedDS.show();
    }

    // salting large dataset key
    // expand small dataset key
    // join both
    public void salting_RDD_join() {
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
}
