package com.spark.feature.bean.dataset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

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
    }
}
