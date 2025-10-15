package org.datapipeline.create_spark;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class lambda_spark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("lambda_spark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        List<String> data1 = Arrays.asList("hello world", "how are you", "flatMap example");

        // --- Tạo RDDs ---
        JavaRDD<Integer> distData = sc.parallelize(data, 2);
        JavaRDD<String> distData1 = sc.parallelize(data1);

        // --- Thực hiện Action ---
        List<Integer> result = distData.collect();
        System.out.println(result);

        // 1. map Transformation (CHUYỂN SANG LAMBDA)
        // rdd = distData.map(lambda i: i * 2)
        JavaRDD<Integer> rdd = distData.map(i -> i * 2);

        System.out.println("map result: " + rdd.collect());

        // 2. filter Transformation (ĐÃ LÀ LAMBDA)
        // rdd1 = distData.filter(lambda i: i > 2)
        JavaRDD<Integer> rdd1 = distData.filter(i -> i > 2);

        System.out.println("filter result: " + rdd1.collect());

        // 3. flatMap Transformation (CHUYỂN SANG LAMBDA)
        // flat_mapped_rdd = distData1.flatMap(lambda line: line.split(" "))
        // Trong Lambda, để trả về Iterator<T>, bạn phải dùng hàm trợ giúp như Arrays.asList().iterator()
        JavaRDD<String> flat_mapped_rdd = distData1.flatMap(
                line -> Arrays.asList(line.split(" ")).iterator()
        );

        System.out.println("flatMap result: " + flat_mapped_rdd.collect());

        // 4. mapPartitions Transformation (CHUYỂN SANG LAMBDA)
        /* Logic: Tính tổng các phần tử trong mỗi partition và trả về tổng đó. */
        JavaRDD<Integer> result1 = distData.mapPartitions(integers -> {
            int sum = 0;
            // Lặp qua tất cả phần tử trong partition
            while (integers.hasNext()) {
                sum += integers.next();
            }
            // Trả về một Iterator chứa tổng (chỉ 1 phần tử)
            return Arrays.asList(sum).iterator();
        });

        System.out.println("mapPartitions result: " + result1.collect());

        sc.stop();
    }
}
