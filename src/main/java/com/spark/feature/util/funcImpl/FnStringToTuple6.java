package com.spark.feature.util.funcImpl;

import org.apache.spark.api.java.function.Function;
import scala.Tuple6;

public class FnStringToTuple6 implements Function<String, Tuple6<String, Integer,String,Integer,String,Integer>> {
    @Override
    public Tuple6<String, Integer,String,Integer,String,Integer> call(String str){
        String[] sArr = str.split(",");
        return new Tuple6<>(sArr[1], Integer.valueOf(sArr[2]), sArr[3], Integer.valueOf(sArr[0]), sArr[5], Integer.valueOf(sArr[6]));
    }
}
