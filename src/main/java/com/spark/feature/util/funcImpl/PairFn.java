package com.spark.feature.util.funcImpl;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class PairFn implements PairFunction<String, String, Integer> {
    public Tuple2<String, Integer> call (String str){
        return new Tuple2<>(str, 1);
    }
}
