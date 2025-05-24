package com.spark.feature.stream;

import com.spark.feature.util.funcImpl.StreamFn;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class StreamRDD {
    private JavaStreamingContext javaStreamingContext;
    private Map<String, Object> kafkaConfig;
    private List<String> topics = Arrays.asList("test-topic");

    public StreamRDD(){
        SparkConf sc = new SparkConf()
                .setAppName("RealTime Processing")
                .setMaster("local[*]");
        javaStreamingContext = new JavaStreamingContext(sc, Minutes.apply(2));

        kafkaConfig = new HashMap<>();
        kafkaConfig.put("bootstrap.servers", "localhost:9092");
        kafkaConfig.put("key.deserializer", StringDeserializer.class);
        kafkaConfig.put("value.deserializer", StringDeserializer.class);
        kafkaConfig.put("group.id", "Spark-kafka-group");
        kafkaConfig.put("auto.offset.reset", "latest");
        kafkaConfig.put("enable.auto.commit", false);
    }

    //batchInterval	Must be ≤ windowDuration
    //windowDuration	Must be ≥ batchInterval
    //slideDuration	Usually equals batchInterval, or a divisor of it
    public void realtimeOperation() throws InterruptedException {
        JavaInputDStream<ConsumerRecord<String, String>> streamContent =
                KafkaUtils.createDirectStream(
                        javaStreamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaConfig));

        JavaPairDStream<String, Long> streamWordmap = streamContent.map(new StreamFn())
                .flatMap(str -> Arrays.stream(str.split(" ")).iterator())
                .mapToPair(st -> new Tuple2<>(st, 1L));
        //it will reduce the values in every minute as per StreamingContext define
        JavaPairDStream<String, Long> streamWordCount = streamWordmap
                .reduceByKey((val1, val2) -> val1+val2);
        streamWordCount.print();
        //it will reduce the value in every 5 min but data will come in every minute
        JavaPairDStream<String, Long> streamWordCountWindow= streamWordmap
                .reduceByKeyAndWindow((val1, val2) -> val1+val2,
                        Durations.minutes(5)); // windowDuration
        streamWordCountWindow.print();
        //reduceByWindow(windowDuration < batchInterval) is invalid or useless
        //Spark cannot process a 1-minute window if it only gets data every 2 minute
        JavaPairDStream<String, Long> slidingWindowWordCount = streamWordmap
                .reduceByKeyAndWindow((v1,v2)->v1+v2,
                        Durations.minutes(5),   // windowDuration
                        Durations.minutes(2));  // slideDuration
        //Here, every 2 minute, Spark will reduce the last 5 minutes of data.

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}