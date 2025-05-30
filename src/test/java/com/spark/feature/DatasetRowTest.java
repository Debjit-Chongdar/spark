package com.spark.feature;

import com.spark.feature.datasetImpl.DatasetRow;
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
        sparkSession.sparkContext().setLogLevel("ERROR");
        datasetRow = new DatasetRow(sparkSession);
    }

    @Test
    public void testFilterOnDataset() {
        datasetRow.filterOnDataset(DatasetRowTest.class.getResource("/DataStore/Dataset/csv/inputEmp.csv").getPath());
    }

    @Test
    public void testInMemoryOperationBySQLAndDatasetFunction() {
        datasetRow.inMemoryOperationBySQLAndDatasetFunction();
    }

    @AfterClass
    public static void tearDown() {
        sparkSession.close();
        sparkSession.stop();
    }
}
