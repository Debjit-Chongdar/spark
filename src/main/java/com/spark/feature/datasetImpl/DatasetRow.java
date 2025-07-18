package com.spark.feature.datasetImpl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class DatasetRow {
    private SparkSession session;
    public DatasetRow(SparkSession sparkSession){
        this.session = sparkSession;
    }

    public void filterOnDataset(String csvPath){
        Dataset<Row> inputDs = session.read().option("header", true).csv(csvPath);
        //filter using expression
        Dataset<Row> goodSeniorDsEx = inputDs.filter("age >= 27 and rating='Good'");
        goodSeniorDsEx.show();
        //filter using Lambda Expression // by default String type, To use >= convert to int
        Dataset<Row> goodSeniorDsLambda = inputDs.filter(ds -> Integer.valueOf(ds.getAs("age"))>=27
                && ds.getAs("rating").equals("Good"));
        goodSeniorDsLambda.show();
        //filter using spark function
        Dataset<Row> goodSeniorDsFn = inputDs.filter(col("age").geq(27)
                .and(col("rating").equalTo("Good"))); //don't use equals here
        goodSeniorDsFn.show();
        //using sql where clause
        inputDs.createOrReplaceTempView("Employee");
        Dataset<Row> goodSeniorDsSql = session.sql("SELECT * FROM Employee WHERE age >= 27 AND rating='Good'");
        goodSeniorDsSql.show();
        session.catalog().dropTempView("Employee");

        /*Scanner sc = new Scanner(System.in);
        sc.nextLine();*/ //enable this too see Spark UI view
    }

    //it will not take value from a file instead in memory
    public void inMemoryOperationBySQLAndDatasetFunction(){
        List<Row> values = new ArrayList<>();
        values.add(RowFactory.create("Ram",6000,"India","2016-12-19 04:39:32"));
        values.add(RowFactory.create("Sam",4000,"India","2018-10-19 04:39:32"));
        values.add(RowFactory.create("Sam",3000,"India","2016-12-19 04:39:32"));
        values.add(RowFactory.create("Ram",6000,"India","2017-11-29 04:39:32"));
        StructField[] structFields = new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("salary", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("nationality", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, true, Metadata.empty())
        };
        Dataset<Row> emplDs = session.createDataFrame(values, new StructType(structFields));
        emplDs.createOrReplaceTempView("Emp_temp");
        Dataset<Row> groupedEmpl = session.sql("SELECT name, sum(salary) as total_sal, " +
                "collect_list(salary) AS sal_list, count(1) AS count from Emp_temp GROUP BY name");
    /*action*/groupedEmpl.show();
        emplDs.createOrReplaceTempView("Emp_temp");
        //dateformat // want to order by month desc
        Dataset<Row> formatedDateDs = session.sql("SELECT name, datetime, " +
                "date_format(datetime, 'YYYY') AS year, date_format(datetime, 'MMM') AS month, " +
                "date_format(datetime, 'MM') AS mn, salary FROM Emp_temp order by mn desc");
        formatedDateDs = formatedDateDs.drop("mn");
    /*action*/formatedDateDs.show();
        session.catalog().dropTempView("Emp_temp");
//----------------------------------- USING DATASET / DATAFRAME --------------------------------------------------
        Dataset<Row> grpDs = emplDs.groupBy("name").agg(
                sum("salary").as("total"),
                collect_list("salary").as("sal_list"), count("salary").as("count")
        );
    /*action*/grpDs.show();
        Dataset<Row> formatedDtDs = emplDs.select("name", "datetime"
                /*,"date_format(datetime, 'DDD')" it will not work*/ );
        //to make it work we have two different way // Using date_format function
        formatedDtDs = formatedDtDs.select(col("name"), col("datetime"),
                date_format(col("datetime"), "YYYY").as("year"),
                date_format(col("datetime"), "MMM").as("month"),
                date_format(col("datetime"), "MM").as("mn"))
                .orderBy(col("mn").desc());
    /*Action*/formatedDtDs.show();
        /*Scanner sc = new Scanner(System.in);
        sc.nextLine();*/ //enable this too see Spark UI view
    }
}
