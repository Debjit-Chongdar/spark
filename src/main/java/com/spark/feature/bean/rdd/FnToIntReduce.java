package com.spark.feature.bean.rdd;

import org.apache.spark.api.java.function.Function2;

public class FnToIntReduce implements Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer val1, Integer val2){
        return val1+val2;
    }
}
