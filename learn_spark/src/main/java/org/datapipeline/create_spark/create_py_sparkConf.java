package org.datapipeline.create_spark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;

public class create_py_sparkConf {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("create_py_sparkConf")
                .setMaster("local[*]");

        // JavaSparkContext là entry point chính cho Spark functionality
        // khi dùng Java RDD API
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Integer> data = Arrays.asList(1,2,3,4,5);
        List<String> data1 = Arrays.asList("hello world", "how are you", "flatMap example");

        // --- Tạo RDDs ---
        // distData được tạo với 2 partitions (quan trọng cho mapPartitions)
        JavaRDD<Integer> distData = sc.parallelize(data, 2);
        JavaRDD<String> distData1 = sc.parallelize(data1);
        // --- Thực hiện Action ---
        List<Integer> result = distData.collect();

        System.out.println(result);

        // --- 1. map Transformation ---
        // rdd = distData.map(lambda i: i * 2)
        JavaRDD<Integer> rdd = distData.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer i) throws Exception {
                return i*2;
            }
        });

        System.out.println("map result: " +rdd.collect());

        // --- 2. filter Transformation ---
        // rdd1 = distData.filter(lambda i: i > 2)
        // Lưu ý: filter trong Java Spark API sử dụng interface FilterFunction
        // Sử dụng Lambda Expression:
        JavaRDD<Integer> rdd1 = distData.filter(i -> i > 2);

        System.out.println("filter result: " +rdd1.collect());

        // --- 3. flatMap Transformation ---
        // flat_mapped_rdd = distData1.flatMap(lambda line: line.split(" "))
        JavaRDD<String> flat_mapped_rdd = distData1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                // Tách chuỗi bằng khoảng trắng và trả về Iterator
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        System.out.println("flatMap result: " + flat_mapped_rdd.collect());

        // --- 4. mapPartitions Transformation ---
        /* def sum_rdd(intergator):
            yield sum(intergator)
        result = distData.mapPartitions(sum_rdd)
        */

        // distData có 2 partitions. mapPartitions tính tổng trên từng partition.
        JavaRDD<Integer> result1 = distData.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Integer> integers) throws Exception {
                int sum = 0;
                while (integers.hasNext()) {
                    sum += integers.next();
                }
                // Chỉ trả về một phần tử duy nhất (tổng) cho partition đó.
                return Arrays.asList(sum).iterator();
            }
        });

        System.out.println("mapPartitions result: " + result1.collect());

        sc.stop();
    }


}
