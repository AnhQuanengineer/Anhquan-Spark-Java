package org.datapipeline.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class test {
    public static final List<Integer> IDS = Arrays.asList(1,2,3,4);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<String> data = Arrays.asList("Hieu", "Bop", "Hien", "Semi aaa");
        // Chia RDD thành 4 phân vùng để đảm bảo mỗi phần tử có chỉ mục ID riêng biệt
        JavaRDD<String> rdd = sc.parallelize(data, 4);

        Function2<Integer, Iterator<String>, Iterator<String>> testPartitionFunc =
                new Function2<Integer, Iterator<String>, Iterator<String>>() {
                    @Override
                    public Iterator<String> call(Integer partitionIndex, Iterator<String> iterator) throws Exception {
                        List<String> partitionData = new ArrayList<>();
                        while (iterator.hasNext()) {
                            partitionData.add(iterator.next());
                        }

                        List<String> results = new ArrayList<>();

                        // Lấy ID khởi tạo dựa trên chỉ mục phân vùng
                        // Ví dụ: Partition 0 -> IDS[0]=1, Partition 1 -> IDS[1]=2, v.v.
                        /*
                        *Hàm này sẽ được gọi 4 lần do có 4 phân vùng(partitions), mỗi partition sẽ chứa data riêng của partition đó
                        * *partitionIndex sẽ tăng lên từ phân vùng 0 -> 3(4 phân vùng) mỗi lần gọi
                        * *vòng for sẽ chỉ sẽ có partitionData.size() là 1 vì nó chỉ chứa 1 cái tên thôi(4 tên chia 4 phân vùng)
                        * *Hàm testPartitionFunc (call(partitionIndex, iterator)) sẽ được gọi 4 lần, mỗi lần trên một executor (hoặc luồng) khác nhau.

                         *A. Xử lý Phân vùng 0 (Executor A)
                         *partitionIndex = 0

                        *startingId = IDS.get(0) = 1

                        *partitionData = ["Hieu"]
                        * *B. Xử lý Phân vùng 1 (Executor B)
                        *partitionIndex = 1

                        *startingId = IDS.get(1) = 2

                        *partitionData = ["Bop"]
                         */
                        int startingId = IDS.get(partitionIndex);

                        System.out.println(startingId);

                        for(int i = 0; i < partitionData.size(); i++){
                            String value = partitionData.get(i);
                            results.add(startingId + ":" +value);
                        }

                        String finalResult = results.stream().collect(Collectors.joining(", ", "[", "]"));

                        // Trả về Iterator của chuỗi kết quả cuối cùng
                        return Arrays.asList(finalResult).iterator();
                    }
        };

        // 4. Áp dụng biến đổi
        JavaRDD<String> result = rdd.mapPartitionsWithIndex(testPartitionFunc, false);

        // 5. Thu thập và In kết quả
        System.out.println(result.collect());

        sc.stop();
    }
}
