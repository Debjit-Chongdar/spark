package com.spark.feature.util.bean;

import java.io.Serializable;

public class Emp implements Serializable {
    private String country;
    private String name;
    private int dept_id;

    public Emp(String country, String name, int dept_id){
        this.country = country;
        this.name = name;
        this.dept_id = dept_id;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public String getCountry() {
        return country;
    }
    public void setCountry(String country) {
        this.country = country;
    }
    public int getDept_id() {
        return dept_id;
    }
    public void setDept_id(int dept_id) {
        this.dept_id = dept_id;
    }
}
