package com.spark.feature.util.funcImpl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.Function;

public class StreamFn implements Function<ConsumerRecord<String, String>, String> {
    @Override
    public String call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
        return stringStringConsumerRecord.value();
    }
}
