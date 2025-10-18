package org.datapipeline.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapPartitionsWithIndex {
    public static void main(String[] args) {
        // 1. Cấu hình Spark và Khởi tạo SparkContext
        // Tương đương với: SparkConf().setAppName("quan dz").setMaster("local[*]")
        SparkConf conf = new SparkConf()
                .setAppName("quan dz")
                .setMaster("local[*]");
        // Tương đương với: sc = SparkContext(conf=conf)
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2. Dữ liệu và Tạo RDD
        // Tương đương với: data = [1, 2, 3, 4, 5]
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        // Tương đương với: rdd = sc.parallelize(data, 2)
        JavaRDD<Integer> rdd = sc.parallelize(data, 2);

        // 3. Định nghĩa hàm xử lý (mapPartitionsWithIndex)
        // Trong Java, bạn sử dụng Function2<Integer, Iterator<T>, Iterator<R>>
        // T<Integer>: Kiểu của phần tử trong RDD đầu vào
        // Integer: Chỉ mục của phân vùng
        // R<String>: Kiểu của phần tử trong RDD đầu ra
        Function2<Integer, Iterator<Integer>, Iterator<String>> processPartitionWithIndex =
                new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
                    @Override
                    public Iterator<String> call(Integer partitionIndex, Iterator<Integer> iterator) throws Exception {
                        List<Integer> elements = new ArrayList<>();
                        while (iterator.hasNext()) {
                            elements.add(iterator.next());
                        }

                        List<String> result = new ArrayList<>();

                        if (partitionIndex == 0) {
                            // Xử lý cho Partition 0: Tăng mỗi phần tử lên 1
                            List<Integer> processedElements = new ArrayList<>();
                            for (int x : elements) {
                                processedElements.add(x + 1);
                            }
                            result.add("Partition " + partitionIndex + ": " + processedElements);
                        } else {
                            // Xử lý cho các Partition khác: Tăng mỗi phần tử lên 2
                            List<Integer> processedElements = new ArrayList<>();
                            for (int x : elements) {
                                processedElements.add(x + 2);
                            }
                            result.add("Partition " + partitionIndex + ": " + processedElements);
                        }

                        // Trả về một Iterator<String>
                        return result.iterator();
                    }
                };

        // 4. Áp dụng phép biến đổi
        // Tương đương với: result = rdd.mapPartitionsWithIndex(process_partition_with_index)
        JavaRDD<String> result = rdd.mapPartitionsWithIndex(processPartitionWithIndex, false); // false = preservePartitioning

        // 5. Thu thập và In kết quả
        // Tương đương với: print(result.collect())
        List<String> finalResult = result.collect();
        System.out.println(finalResult);

        // Đóng SparkContext
        sc.stop();

        /*
        *Chữ ký Bắt buộc của Spark API: Phương thức mapPartitionsWithIndex trong JavaRDD được định nghĩa sẵn để chấp nhận một hàm triển khai giao diện Function2<Integer, Iterator<T>, Iterator<R>>. Đây là quy tắc cố định của API:
        * *Đầu ra phải là Iterator<R>: Spark yêu cầu giá trị trả về phải là một Iterator (trong trường hợp của bạn là Iterator<String>) để có thể xử lý các phần tử đầu ra một cách lười biếng (lazily) và hiệu quả về bộ nhớ. Spark không muốn tải toàn bộ danh sách kết quả của một phân vùng vào bộ nhớ trước khi xử lý phân vùng tiếp theo.
         */
    }
}
