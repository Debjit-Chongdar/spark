package com.spark.feature;

import com.spark.feature.rdd.JavaRDDImpl;
import in.apache.spark.controller.AppController;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class JavaRDDImplTest {
    private static SparkSession sparkSession;
    private static JavaRDDImpl javaRDDImpl;

    @BeforeClass
    public static void setUp() {
        sparkSession = SparkSession
                .builder()
                .appName("AppTest")
                .master("local[*]")
                //.config("spark.testing.memory", "2147480000")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        javaRDDImpl = new JavaRDDImpl(sparkSession);
    }

    @Test
    public void testOperation(){
        List<String> bookList = new ArrayList<>();
        bookList.add("a1");
        bookList.add("a2");
        bookList.add("b1");
        bookList.add("b3");
        bookList.add("a2");
        bookList.add("b4");
        bookList.add("a2");
        bookList.add("b3");
        javaRDDImpl.operationOnRDD(bookList);
    }

    @Test
    public void testJoinOperationOnRDD(){
        String path1 = JavaRDDImplTest.class.getResource("/DataStore/Dataset/text/inputEmp.txt").getPath();
        String path2 = JavaRDDImplTest.class.getResource("/DataStore/Dataset/text/inputDept.txt").getPath();
        javaRDDImpl.joinOperationOnRDD(path1, path2);
    }

    @AfterClass
    public static void tearDown() {
        sparkSession.close();
        sparkSession.stop();
    }
}
