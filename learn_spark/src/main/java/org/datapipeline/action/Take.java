package org.datapipeline.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Take {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Take").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Integer> data = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        JavaRDD<Integer> rdd = sc.parallelize(data,2);

        List<Integer> result = rdd.take(6);
        System.out.println(result);
        sc.stop();

    }
}
