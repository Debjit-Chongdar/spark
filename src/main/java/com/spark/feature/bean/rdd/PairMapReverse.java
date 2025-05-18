package com.spark.feature.bean.rdd;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class PairMapReverse implements PairFunction<Tuple2<String, Integer>, Integer, String> {
    @Override
    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2){
        return new Tuple2<>(tuple2._2, tuple2._1);
    }
}
