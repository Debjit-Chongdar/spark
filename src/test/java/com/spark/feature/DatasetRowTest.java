package com.spark.feature;

import com.spark.feature.bean.dataset.DatasetRow;
import com.spark.feature.rdd.JavaRDDImpl;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatasetRowTest {
    private static SparkSession sparkSession;
    private static DatasetRow datasetRow;

    @BeforeClass
    public static void setUp() {
        sparkSession = SparkSession
                .builder()
                .appName("AppTest")
                .master("local[*]")
                //.config("spark.testing.memory", "2147480000")
                .getOrCreate();
        datasetRow = new DatasetRow(sparkSession);
    }

    @Test
    public void testFilterOperation(){
        datasetRow.filterOperation(DatasetRowTest.class.getResource("/DataStore/Dataset/csv/inputEmp.csv").getPath());
    }

    @Test
    public void testInMemoryOperation(){
        datasetRow.inMemoryOperation();
    }

    @AfterClass
    public static void tearDown() {
        sparkSession.close();
        sparkSession.stop();
    }
}
