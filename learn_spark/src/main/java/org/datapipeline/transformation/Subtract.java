package org.datapipeline.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Subtract {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Subtract").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<String> data1 = Arrays.asList("toI chiM tO quA");
        List<String> data2 = Arrays.asList("chiM quA");

        JavaRDD<String> rdd1 = sc.parallelize(data1);
        JavaRDD<String> rdd2 = sc.parallelize(data2);

        JavaRDD<String> rdd1_flatmap = rdd1.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaRDD<String> rdd2_flatmap = rdd2.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaRDD<String> rdd1_lower = rdd1_flatmap.map(x -> x.toLowerCase());
        JavaRDD<String> rdd2_lower = rdd2_flatmap.map(x -> x.toLowerCase());

        JavaRDD<String> result = rdd1_lower.subtract(rdd2_lower);
        System.out.println(result.collect());

        sc.stop();

    }
}
