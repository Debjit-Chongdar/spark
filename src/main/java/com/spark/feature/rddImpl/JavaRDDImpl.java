package com.spark.feature.rddImpl;

import com.spark.feature.util.funcImpl.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;
import scala.Tuple6;

import java.util.List;

public class JavaRDDImpl {
    private SparkSession session;
    private JavaSparkContext jsc;

    public JavaRDDImpl(SparkSession sparkSession){
        this.session = sparkSession;
        this.jsc = new JavaSparkContext(sparkSession.sparkContext());
    }
    //map(Function(),Function2()...)->JavaRDD, mapToPair(PairFunction())->JavaPairRDD,
    // reduce()->Object, reduceByKey()->JavaPairRDD,
    // groupByKey()->JavaPairRDD<Object, Iterable<Object>>,
    // sortByKey(true/false) -> JavaPairRDD, first()->Object/Tuple2, foreach()
    public void operationOnRDD(List<String> bookList){
        JavaRDD<String> booksRdd = jsc.parallelize(bookList);
        //Count of all books
    /*Action*/Integer bookCnt = booksRdd.map(new FnStringTo1()).reduce(new FnToIntReduce());
        System.out.println("count (using function) = "+bookCnt); //--------------------------------
    /*Action*/int bookCount = booksRdd.map(str -> 1).reduce((a,b) -> a+b);
        System.out.println("count (using Lambda Expression) = "+bookCount); //-------------------------------------------
        //Count of each book
        JavaPairRDD<String, Integer> bookPairRdd = booksRdd.mapToPair(new PairFn());
    /*suffle*/JavaPairRDD<String, Integer> bookcntRdd = bookPairRdd.reduceByKey(new FnToIntReduce());
        System.out.println("count of each book reduceBy (Using Function) "); //---------------------------------------------
    /*Action*/bookcntRdd.foreach(bean -> System.out.println(bean._1+"  "+bean._2));
        JavaPairRDD<String, Integer> bookCountRdd = booksRdd.mapToPair(str -> new Tuple2<>(str, 1))
    /*suffle*/     .reduceByKey((val1, val2)-> val1+val2);
        System.out.println("count of each book reduceBy (Using Lambda Expression) "); //---------------------------------------------
    /*Action*/bookCountRdd.foreach(bean -> System.out.println(bean._1+"  "+bean._2));

    /*suffle*/JavaPairRDD<String, Iterable<Integer>> bookGrpRdd = bookPairRdd.groupByKey();
        JavaPairRDD<String, Integer> bookGrpCountRdd = bookGrpRdd.mapToPair(new PairOnGroupBy());
        System.out.println("Group By count (using function) ");
    /*Action*/bookGrpCountRdd.foreach(bean -> System.out.println(bean._1+"  "+bean._2));
        System.out.println("Group By count (using Lambda Expression) ");
    /*suffle*/booksRdd.mapToPair(str -> new Tuple2<>(str, 1)).groupByKey()
    /*Action*/        .foreach(bean-> System.out.println(bean._1+"  "+bean._2));
        //Book is having max count
        JavaPairRDD<Integer, String> cntBookRDD = bookCountRdd.mapToPair(new PairMapReverse())
    /*suffle*//*Action*/.sortByKey(false); // false means descending order
    /*Action*/Tuple2<Integer, String> maxCountBook = cntBookRDD.first();
        System.out.println("Max book count (Using Function) "+maxCountBook._2);
        // returning wrong result as sout going to different executer and execute asyncron// returning wrong result asusly
    /*Action*/cntBookRDD.values().foreach(bean -> System.out.println(bean));
        System.out.println("----------------- Using Lambda expression ----------------");
        bookCountRdd.mapToPair(tuple2 -> new Tuple2<>(tuple2._2, tuple2._1))
    /*suffle*//*Action*//*Action*/.sortByKey(false).values().foreach(bean -> System.out.println(bean));
        /*Scanner sc = new Scanner(System.in);
        sc.nextLine();*/ //this part is to view in spark ui
    }
    // Join can be done only on JavaPairRDD on key
    public void joinOperationOnRDD(String path1, String path2){
        /*stage-0*/JavaRDD<String> rdd1 = jsc.textFile(path1);
        /*stage-1*/JavaRDD<String> rdd2 = jsc.textFile(path2);
        //rdd 1 has 6 field in it
        JavaRDD<Tuple6<String, Integer,String,Integer,String,Integer>> empRdd = rdd1.map(new FnStringToTuple6());
        //dept has only 2 field/column
        JavaPairRDD<Integer, String> deptIdRdd = rdd2.map(str->str.split(","))
                .mapToPair(str-> new Tuple2<>(Integer.valueOf(str[0]), str[1])); // this will use in two place which create another stage
        JavaPairRDD<Integer, Tuple5<String, Integer,String,Integer,String>> empIdRdd = empRdd
                .mapToPair(tuple6 -> new Tuple2<>(tuple6._6(),
                        new Tuple5<>(tuple6._1(), tuple6._2(), tuple6._3(), tuple6._4(), tuple6._5())));
        JavaPairRDD<Integer, Tuple2<Tuple5<String, Integer,String,Integer,String>, String>> joinedRdd =
        /*suffle*/empIdRdd.join(deptIdRdd); //join on dept id
        JavaRDD<Tuple2<Tuple5<String, Integer,String,Integer,String>, String>> deptJoinRDD = joinedRdd.values();
        //dept in key, other cols in value from the values of PairRDD
        JavaPairRDD<String, Integer> deptSalJoinPairRdd = deptJoinRDD
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2, tuple2._1._2()));
        //Find dept name wise Average Salary
        /*suffle*/JavaPairRDD<String, Double> deptAvjSalRdd = deptSalJoinPairRdd.groupByKey()
                .mapToPair(new PairOnGroupByToAvg());
        //reverse key value in deptIdRdd
        JavaPairRDD<String, Integer> deptNameRdd = deptIdRdd
                .mapToPair(tuple2 -> new Tuple2<>(tuple2._2, tuple2._1));
        //Join to get dept id, name, avg salary in a single row
        /*suffle*/JavaRDD<Tuple3<Integer, String, Double>> rdd = deptAvjSalRdd.join(deptNameRdd)
                .map(tuple2 -> new Tuple3<>(tuple2._2._2, tuple2._1, tuple2._2._1));
        /*action*/rdd.collect().forEach(System.out::println);
        /*Scanner sc = new Scanner(System.in);
        sc.nextLine();*/ //enable this part is to view in spark ui
    }
}
