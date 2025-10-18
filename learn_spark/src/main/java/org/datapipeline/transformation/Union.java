package org.datapipeline.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Union {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Intersection").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Integer> data1 = Arrays.asList(1,2,3,4,5);
        List<Integer> data2 = Arrays.asList(6,1,2,9,10);

        JavaRDD<Integer> rdd1 = sc.parallelize(data1);
        JavaRDD<Integer> rdd2 = sc.parallelize(data2);

        JavaRDD<Integer> union = rdd1.union(rdd2);
        System.out.println(union.collect());

        sc.stop();
    }
}
