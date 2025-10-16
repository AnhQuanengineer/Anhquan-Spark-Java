package org.datapipeline.key_value.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReduceByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ReduceByKey")
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

        JavaPairRDD<String,Integer> data = sc.parallelizePairs(list);

        // --- Bước 1: reduceByKey ---
        // Tương đương với: bill = data.reduceByKey(lambda key,value:key+value)

        // Định nghĩa hàm cộng (Function2<Giá trị, Giá trị, Giá trị Kết quả>)
//        Function2<Integer, Integer, Integer> sumFunction = (a, b) -> a + b;
//        JavaPairRDD<String, Integer> bill = data.reduceByKey(sumFunction);

        JavaPairRDD<String, Integer> bill = data.reduceByKey((Integer a, Integer b) -> a + b);
//        JavaPairRDD<String, Integer> reduceByKey = data.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            // Phương thức call phải được ghi đè
//            @Override
//            public Integer call(Integer value1, Integer value2) throws Exception {
//                // Thêm logic tính tổng vào đây
//                return value1 + value2;
//            }
//        });
//        JavaPairRDD<String, Integer> bill = data.reduceByKey((a, b) -> a + b);

        System.out.println(bill.collect());

        JavaPairRDD<Integer, String> swappedBill = bill.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));
         //Sử dụng Anonymous Class:
//        JavaPairRDD<Integer, String> swappedBill = bill.mapToPair(
//                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
//
//                    /**
//                     * Phương thức call phải được ghi đè để định nghĩa logic map.
//                     * Nhận vào: Tuple2<String, Integer> (Tên, Tổng)
//                     * Trả về: Tuple2<Integer, String> (Tổng, Tên)
//                     */
//                    @Override
//                    public Tuple2<Integer, String> call(Tuple2<String, Integer> pair) throws Exception {
//                        // pair._2() là Giá trị (Tổng), pair._1() là Khóa (Tên)
//                        return new Tuple2<>(pair._2(), pair._1());
//                    }
//                }
//        );
        JavaPairRDD<Integer, String> sortBill = swappedBill.sortByKey(false);

        System.out.println(sortBill.collect());

    }
}
