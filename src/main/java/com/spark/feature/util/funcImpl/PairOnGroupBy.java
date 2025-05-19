package com.spark.feature.util.funcImpl;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class PairOnGroupBy implements PairFunction<Tuple2<String, Iterable<Integer>>, String, Integer> {
    public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> tuple2){
        return new Tuple2<>(tuple2._1, Iterables.size(tuple2._2));
    }
}
