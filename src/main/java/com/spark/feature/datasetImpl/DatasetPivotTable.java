package com.spark.feature.datasetImpl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.util.Scanner;

import static org.apache.spark.sql.functions.col;

public class DatasetPivotTable {
    private SparkSession session;
    public DatasetPivotTable(SparkSession sparkSession){
        this.session = sparkSession;
    }

    public void convertToPivotTable(String path){
        Dataset<Row> empDs = session.read().option("header", true).csv(path);

    //Simple order by in Dataset
        System.out.println("-----------  Dataset # order by name, sal desc --------------");
        empDs.orderBy(col("name").desc(), col("sal").desc()).show();
    //Simple group by   **   SQL
        empDs.createOrReplaceTempView("employees");
        Dataset<Row> deptWiseMaxSalDS = session.sql("select dept_id, rating, " +
                "max(sal) as max_sal, avg(age) as avg_age from employees group by dept_id, rating");
        session.catalog().dropTempView("employees");
        System.out.println("--------  SQL  #  Group by ----------");
        deptWiseMaxSalDS.show();
    //Simple groupBy  **  Dataset
        Dataset<Row> groupedDs = empDs.groupBy("dept_id", "rating").agg(
                functions.avg("sal").as("avg_sal"), functions.avg("age").as("avg_age")
        );
        System.out.println("--------  Dataset  #  Group by ----------");
        groupedDs.show();

    //Pivot with GroupBy
        Dataset<Row> avgSalOnDeptAndRating = empDs.groupBy("dept_id").pivot("rating")
                .agg(functions.avg("sal").as("avg_sal"));
        System.out.println("---------  Dataset  groupBy(dept_id).pivot(rating).agg(avg(sal)) ------------");
        avgSalOnDeptAndRating.show();
    //Pivot with Multiple Aggregation label
        Dataset<Row> avgSalAndAgeOnDeptAndRating = empDs.groupBy("dept_id").pivot("rating")
                .agg(functions.avg("sal").as("avg_sal"),functions.avg("age").as("avg_age"));
        System.out.println("--------- groupBy(dept_id).pivot(rating).agg(avg(sal), avg(age)) ------------");
        avgSalAndAgeOnDeptAndRating.show();

    //Window function  **  Dataset
        WindowSpec partitionByOrder = Window.partitionBy("dept_id").orderBy(col("sal").desc());
        Dataset<Row> allRank = empDs
                .withColumn("row_number", functions.row_number().over(partitionByOrder))
                .withColumn("dense_rank", functions.dense_rank().over(partitionByOrder))
                .withColumn("rank", functions.rank().over(partitionByOrder));
        System.out.println("---------- Window.partitionBy().orderBy(col().desc) -----------");
        allRank.show();
    //row_number() over partition by  **  Dataset
        Dataset<Row> deptWiseMaxSal = empDs.select("dept_id", "name", "sal","rating")
                .withColumn("row_num", functions.row_number().over(
                    Window.partitionBy("dept_id").orderBy(col("sal").desc()))
                ).where(col("row_num").equalTo(1)).drop("row_num");
        System.out.println("------------  Dataset   #  row_number() over partionBy, orderBy --------");
        deptWiseMaxSal.show();
    //row_number() over partition by  **  SQL
        empDs.createOrReplaceTempView("emps");
        Dataset<Row> rownumEmpds = session.sql("with temp as " +
                "(select dept_id, name, country, rating, sal, " +
                "row_number() over (partition by dept_id order by sal desc) as rn from emps) " +
                "select dept_id, name, sal, rating from temp where rn=1");
        session.catalog().dropTempView("emps");
        System.out.println("-------- SQL # row_number over partition by, order by --------");
        rownumEmpds.show();

        Scanner sc = new Scanner(System.in);
        sc.nextLine();
    }
}
