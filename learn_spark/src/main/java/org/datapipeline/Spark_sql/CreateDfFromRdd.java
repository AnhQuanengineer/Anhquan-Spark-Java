package org.datapipeline.Spark_sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext; // Import cần thiết
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class CreateDfFromRdd implements Serializable {

    public static void main(String[] args) {
        // 1. Khởi tạo SparkSession
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Quan dz")
                .getOrCreate();

        // **KHẮC PHỤC LỖI:** Lấy JavaSparkContext từ SparkSession để sử dụng các API Java.
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("ERROR");

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Sử dụng jsc.parallelize() để chấp nhận java.util.List
        JavaRDD<Integer> rdd = jsc.parallelize(data, 4);

        // 2. map(lambda x: (x, random.randint(1, 1000) * x))
        JavaPairRDD<Integer, Integer> pairRDD = rdd.mapToPair(x -> {
            // Tối ưu: Khởi tạo Random bên trong để đảm bảo tính ngẫu nhiên phân tán
            Random localRandom = new Random();
            int randomValue = localRandom.nextInt(1000) + 1;

            return new Tuple2<>(x, randomValue * x);
        });

        // Chuyển đổi JavaPairRDD sang JavaRDD<Row>
        JavaRDD<Row> rowRDD = pairRDD.map(pair ->
                RowFactory.create(pair._1(), pair._2())
        );

        // 3. Định nghĩa Schema
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("number", DataTypes.IntegerType, false)
        });

        // 4. df = spark.createDataFrame(rdd, schema).show()
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        df.show();

        spark.stop();
    }
}