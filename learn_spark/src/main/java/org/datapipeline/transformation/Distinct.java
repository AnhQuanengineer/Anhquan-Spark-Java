package org.datapipeline.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Distinct {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Distinct").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Integer> data = Arrays.asList(1, 2, 3, 3, 5, 6, 6, 8, 9, 10);

        JavaRDD<Integer> rdd = sc.parallelize(data);

        List<Integer> result = rdd.distinct().collect();

        System.out.println(result);

        sc.stop();

    }
}
