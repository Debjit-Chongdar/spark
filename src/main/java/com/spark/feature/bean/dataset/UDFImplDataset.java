package com.spark.feature.bean.dataset;

import com.spark.feature.dataset.EmployeeResultUDF2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.col;

public class UDFImplDataset {
    private SparkSession session;
    public UDFImplDataset(SparkSession sparkSession){
        this.session = sparkSession;
    }

    public void useUdfInSql(String path) {
        Dataset<Row> empDs = session.read().option("header", true).csv(path);
        empDs.createOrReplaceTempView("emp_view");
        //using Class return pass/fail
        session.udf().register("empResultUDF", new EmployeeResultUDF2(), DataTypes.StringType);
        Dataset<Row> redultDs = session.sql("select name, rating, age, empResultUDF(rating, age) as result from emp_view");
        redultDs.show();

        //Using Lambda expression
        UDF2<String, String, String> resultUdf = (String rating, String age) -> {
            if (Integer.valueOf(age) > 27) {
                return (rating.equalsIgnoreCase("poor")) ? "Fail" : "Pass";
            } else {
                return rating.equalsIgnoreCase("very good") || rating.equals("Excelent") ? "Pass" : "Fail";
            }
        };
        session.udf().register("resultUDF", resultUdf, DataTypes.StringType);
        Dataset<Row> redultDs1 = empDs.select("name", "rating", "age")
                .withColumn("result",
                        functions.callUDF("resultUDF", col("rating"), col("age")));
        redultDs1.show();
    }
}
