package org.datapipeline.key_value.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Lookup {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Lookup.class.getName())
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Tuple2<String, Integer>> list = Arrays.asList(
                new Tuple2<String, Integer>("quan",2),
                new Tuple2<String, Integer>("minh anh",4),
                new Tuple2<String, Integer>("quan",6),
                new Tuple2<String, Integer>("minh anh",8),
                new Tuple2<String, Integer>("victo",10)
        );

        JavaPairRDD<String, Integer> data = sc.parallelizePairs(list);

        List<Integer> result = data.lookup("quan");

        System.out.println(result);

        sc.stop();
    }
}
