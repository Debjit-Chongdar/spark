package com.spark.feature;

import com.spark.feature.datasetImpl.UDFImplDataset;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class UDFImplDatasetTest {
    private static UDFImplDataset udfImplDataset;
    private static SparkSession sparkSession;

    @BeforeClass
    public static void setUp() {
        sparkSession = SparkSession
                .builder()
                .appName("DatasetPivotTable Test")
                .master("local[*]")
                //.config("spark.testing.memory", "2147480000")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        udfImplDataset = new UDFImplDataset(sparkSession);
    }

    @Test
    public void testUseUdfInSql(){
        udfImplDataset.useUdfInSql(DatasetRowTest.class.getResource("/DataStore/Dataset/csv/inputEmp.csv").getPath());
    }

    @AfterClass
    public static void tearDown() {
        sparkSession.close();
        sparkSession.stop();
    }
}
