package com.spark.feature.salting.bean;

import org.apache.spark.sql.api.java.UDF0;

import java.util.Random;

public class RandomUdf implements UDF0<Integer> {
    @Override
    public Integer call(){
        return new Random().nextInt(5);
    }
}
