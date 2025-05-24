package com.spark.feature.stream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.*;

public class StreamDataset {
    private SparkSession sparkSession;

    public StreamDataset(){
        sparkSession = SparkSession.builder().master("local[*]").appName("Stream DS").getOrCreate();
    }

    public void streamOperation() throws StreamingQueryException {
        Dataset<Row> streamDs = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test-topic")
                .load();

        // Convert value from binary to string
        Dataset<Row> messages = streamDs.selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING())
                .toDF("message");

        // Simple transformation: word count
        Dataset<Row> words = messages
                .select(explode(split(col("message"), " ")).alias("word"))
                .groupBy("word")
                .count();

        // Write result to console
        words.writeStream()
                .outputMode(OutputMode.Complete()) // or "update"
                .format("console")
                .option("truncate", false)
                .start()
                .awaitTermination();
    }
}
