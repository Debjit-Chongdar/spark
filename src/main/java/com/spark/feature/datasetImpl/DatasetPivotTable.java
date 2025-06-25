package com.spark.feature.datasetImpl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

public class DatasetPivotTable {
    private SparkSession session;
    public DatasetPivotTable(SparkSession sparkSession){
        this.session = sparkSession;
    }
    //Department and Rating wise avg salary
    public void convertToPivotTable(String path){
        Dataset<Row> empDs = session.read().option("header", true).csv(path);
        Dataset<Row> groupedDs = empDs.groupBy("dept_id", "rating").agg(
                functions.avg("sal").as("avg_sal"), functions.avg("age").as("avg_age")
        );
        groupedDs.show();
        Dataset<Row> avgSalOnDeptAndRating = empDs.groupBy("dept_id").pivot("rating").agg(functions.avg("sal"));
        avgSalOnDeptAndRating.show();
        Dataset<Row> avgSalAndAgeOnDeptAndRating = empDs.groupBy("dept_id").pivot("rating").agg(
                functions.avg("sal").as("avg_sal"),functions.avg("age").as("avg_age")
        );
        avgSalAndAgeOnDeptAndRating.show();

        //Window function
        Dataset<Row> deptWiseMaxSal = empDs.withColumn("row_num", functions.row_number().over(
                Window.partitionBy("dept_id").orderBy(col("sal").desc())))
                .filter(col("row_num").equalTo(1)).drop("row_num");
        deptWiseMaxSal.show();
    }
}
