package com.spark.feature;

import com.spark.feature.salting.Salting;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.junit.Test;

public class SaltingTest {
    private static SparkSession session;

    @BeforeClass
    public static void setup(){
        session = SparkSession.builder().master("local[*]").appName("Salting_Test").getOrCreate();
        session.sparkContext().setLogLevel("ERROR");
    }

    @Test
    public void test_saltingOnDs(){
        String empPath = new SaltingTest().getClass().getResource("/DataStore/Dataset/DsDFSource/emp/employee.json").getPath();
        String deptPath = new SaltingTest().getClass().getResource("/DataStore/Dataset/DsDFSource/dept/dept.json").getPath();
        new Salting(session).saltingOnDs(empPath, deptPath);
    }
}
