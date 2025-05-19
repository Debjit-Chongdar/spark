package com.spark.feature;

import com.spark.feature.datasetImpl.DatasetPivotTable;
import com.spark.feature.datasetImpl.DatasetRow;
import com.spark.feature.datasetImpl.UDFImplDataset;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatasetRowTest {
    private static SparkSession sparkSession;
    private static DatasetRow datasetRow;
    private static DatasetPivotTable datasetPivotTable;
    private static UDFImplDataset udfImplDataset;

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
        datasetPivotTable = new DatasetPivotTable(sparkSession);
        udfImplDataset = new UDFImplDataset(sparkSession);
    }

    @Test
    public void testFilterOnDataset(){
        datasetRow.filterOnDataset(DatasetRowTest.class.getResource("/DataStore/Dataset/csv/inputEmp.csv").getPath());
    }

    @Test
    public void testInMemoryOperationBySQLAndDatasetFunction(){
        datasetRow.inMemoryOperationBySQLAndDatasetFunction();
    }

    @Test
    public void testConvertToPivotTable(){
        datasetPivotTable.convertToPivotTable(DatasetRowTest.class.getResource("/DataStore/Dataset/csv/inputEmp.csv").getPath());
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
