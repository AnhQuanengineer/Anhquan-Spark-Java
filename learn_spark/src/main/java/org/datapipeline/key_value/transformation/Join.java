package org.datapipeline.key_value.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Join {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Join")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        // 2. Định nghĩa dữ liệu đầu vào cho RDD 1
        List<Tuple2<Integer, Double>> list1 = Arrays.asList(
                new Tuple2<>(110, 50.12),
                new Tuple2<>(127, 90.5),
                new Tuple2<>(126, 211.0),
                new Tuple2<>(105, 6.0),
                new Tuple2<>(165, 31.0),
                new Tuple2<>(110, 40.11)
        );

        // 3. Tạo JavaPairRDD 1 (Khóa: Integer, Giá trị: Double)
        JavaPairRDD<Integer, Double> data1 = sc.parallelizePairs(list1);

        // 4. Định nghĩa dữ liệu đầu vào cho RDD 2
        List<Tuple2<Integer, String>> list2 = Arrays.asList(
                new Tuple2<>(110, "dat"),
                new Tuple2<>(127, "kun"),
                new Tuple2<>(126, "heu kkk"),
                new Tuple2<>(105, "khanh sky"),
                new Tuple2<>(165, "dung troc")
        );

        // 5. Tạo JavaPairRDD 2 (Khóa: Integer, Giá trị: String)
        JavaPairRDD<Integer, String> data2 = sc.parallelizePairs(list2);

        // 6. Thực hiện phép JOIN tương đương với dataNew = data1.join(data2)
        JavaPairRDD<Integer, Tuple2<Double, String>> dataNew = data1.join(data2);

        // 7. Thu thập kết quả và in ra tương đương với for result in dataNew.collect(): print(result)
        List<Tuple2<Integer, Tuple2<Double, String>>> results =dataNew.collect();

        for(Tuple2<Integer, Tuple2<Double, String>> result : results) {
            System.out.println(result);
        }

        sc.stop();
    }
}
