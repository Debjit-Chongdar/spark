package com.spark.feature.dataset;

import org.apache.spark.sql.api.java.UDF2;

public class EmployeeResultUDF2 implements UDF2<String, String, String> {
    @Override
    public String call(String rating, String age) {
        if (Integer.valueOf(age) > 27) {
            return (rating.equalsIgnoreCase("poor")) ? "Fail" : "Pass";
        } else {
            return rating.equalsIgnoreCase("very good") || rating.equals("Excelent") ? "Pass" : "Fail";
        }
    }
}
