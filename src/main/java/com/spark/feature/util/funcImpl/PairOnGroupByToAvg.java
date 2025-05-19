package com.spark.feature.util.funcImpl;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class PairOnGroupByToAvg implements PairFunction<Tuple2<String, Iterable<Integer>>, String, Double> {
    @Override
    public Tuple2<String, Double> call(Tuple2<String, Iterable<Integer>> tuple2){
        int count = Iterables.size(tuple2._2);
        Integer sum =0;
        for (Integer in : tuple2._2){
            sum+=in;
        }
        Double avg = (double)sum/count;
        return new Tuple2<>(tuple2._1, avg);
    }
}
