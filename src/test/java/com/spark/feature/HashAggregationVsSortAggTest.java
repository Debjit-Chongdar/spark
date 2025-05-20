package com.spark.feature;

import com.spark.feature.datasetImpl.HashAggregationVsSortAgg;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HashAggregationVsSortAggTest {
    private static HashAggregationVsSortAgg hashAggregationVsSortAgg;
    private static SparkSession sparkSession;

    @BeforeClass
    public static void setUp(){
        sparkSession = SparkSession.builder()
                .appName("HashAggregationVsSortAgg").master("local[*]").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        hashAggregationVsSortAgg = new HashAggregationVsSortAgg(sparkSession);
    }

    @Test
    public void testHashAggregationAndSortAggregation(){
        hashAggregationVsSortAgg.hashAggregationAndSortAggregation();
    }

    @AfterClass
    public static void tearDown(){
        sparkSession.stop();
        sparkSession.close();
    }
}
