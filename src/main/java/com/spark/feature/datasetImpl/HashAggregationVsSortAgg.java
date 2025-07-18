package com.spark.feature.datasetImpl;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class HashAggregationVsSortAgg {
    private SparkSession session;

    public HashAggregationVsSortAgg(SparkSession sparkSession){
        this.session = sparkSession;
    }

    public void hashAggregationAndSortAggregation(){
        Dataset<Row> ds = getData();
        ds.createOrReplaceTempView("user_view");
        Dataset<Row> grpDs = session.sql("SELECT name, first(date_format(datetime, 'MM')) AS month FROM" +
                " user_view GROUP BY name ORDER BY month");
        // this one use "sortAggregate" as month dataType is String and it's Immutable (Type-Varchar)
        grpDs.explain();
        //---------------- if we pass month in group by ----------------------------------------------
        Dataset<Row> grpMnDs = session.sql("SELECT name, date_format(datetime, 'MM') AS month FROM" +
                " user_view GROUP BY name, date_format(datetime, 'MM') ORDER BY month");
        //This one is used HashAggregate as we have use order by column in group by part as well
        grpMnDs.explain();
        //--------------- if we can type cast month to Integer ---------------------------------------
        Dataset<Row> grpIntDs = session.sql("SELECT name, first(CAST(date_format(datetime, 'MM') AS INTEGER)) AS month " +
                "FROM user_view GROUP BY name ORDER BY month");
        // this one use "HashAggregate" as month dataType is int and it's Mutable (Type-int-32bit fixed size)
        grpIntDs.explain();
        session.catalog().dropTempView("user_view");
        // --------------- this is Fastest ------------------------------------------------------------
        Dataset<Row> fastExecutionDs = ds.groupBy("name").agg(
                functions.first(
                        functions.date_format(functions.col("datetime"), "MM").cast(DataTypes.IntegerType)
                ).as("month")
        );
        // it will use "HashAggregate" as TypeCast it to Integer and also using JavaAPI, So it will be fastest
        fastExecutionDs.explain();
        /*Scanner sc = new Scanner(System.in);
        sc.nextLine();*/ //Uncomment to see in Spark UI
    }

    private Dataset<Row> getData(){
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
        return emplDs;
    }
}
