package org.datapipeline.key_value.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CollectAsMap {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CollectAsMap").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("quan",2),
                new Tuple2<String, Integer>("minh anh",4),
                new Tuple2<String, Integer>("quan",6),
                new Tuple2<String, Integer>("minh anh",8),
                new Tuple2<String, Integer>("victo",10)
        );

        JavaPairRDD<String,Integer> pairRDD = sc.parallelizePairs(list);
        Map<String, Integer> map = pairRDD.collectAsMap();

        System.out.println(map);
        sc.stop();
    }
}
