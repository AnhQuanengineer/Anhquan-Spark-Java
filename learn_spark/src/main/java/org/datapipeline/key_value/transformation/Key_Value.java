package org.datapipeline.key_value.transformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


public class Key_Value {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Key_Value")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<String> data = Arrays.asList("Anh Quan dep zai qua");

        // Create rdd
        JavaRDD<String> rdd = sc.parallelize(data);

        //Transformation
        JavaRDD<String> rdd2 = rdd.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        );

        System.out.println("flatMap result: " + rdd2.collect());

        JavaPairRDD<Integer, String> rdd3 = rdd.mapToPair(
                i -> new Tuple2<>(i.length(), i)
        );

        System.out.println("map result: " + rdd3.collect());

        JavaPairRDD<Integer, Iterable<String>> groupByKeyRdd = rdd3.groupByKey();
        System.out.println("groupByKey result: " + groupByKeyRdd.collect());

        sc.stop();
    }
}
