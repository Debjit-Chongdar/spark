package com.spark.feature.util.funcImpl;

import org.apache.spark.api.java.function.Function;

public class FnStringTo1 implements Function<String,Integer> {
    @Override
    public Integer call(String s) throws Exception {
        return 1;
    }
}
