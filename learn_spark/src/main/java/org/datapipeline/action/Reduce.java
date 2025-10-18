package org.datapipeline.action;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class Reduce {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Reduce").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Integer> data = Arrays.asList(1,2,3,4,5,6,7,8,9,10);

        JavaRDD<Integer> rdd = sc.parallelize(data,2);

//        Function2<Integer,Integer,Integer>sum = (a,b) -> a+b;
        Function2<Integer,Integer,Integer>sum =
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        System.out.println("t1:" + integer + " + t2:" + integer2 + " = " + (integer + integer2) );
                        return integer + integer2;
                    }
                };

        Integer result = rdd.reduce(sum);

        System.out.println(result);
        sc.close();
    }
}
