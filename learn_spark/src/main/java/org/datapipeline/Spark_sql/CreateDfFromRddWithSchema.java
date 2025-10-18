package org.datapipeline.Spark_sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class CreateDfFromRddWithSchema {
    public static void main(String[] args) {
        // 1. Khởi tạo SparkSession
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Quan dz")
                .getOrCreate();

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("ERROR");

        // 2. Tạo dữ liệu tĩnh (tương đương với sparkContext.parallelize([Row(...)]))
        // Dữ liệu phải được tạo dưới dạng List<Row> bằng RowFactory.create()
        List<Row> data = Arrays.asList(
                RowFactory.create(1, "dat", 24),
                RowFactory.create(2, "quan", 18),
                RowFactory.create(3, "hoang", 22)
        );

        // Chuyển List<Row> thành JavaRDD<Row>
        JavaRDD<Row> rowRDD = jsc.parallelize(data);

        // 3. Định nghĩa Schema (StructType)
        StructType schema = DataTypes.createStructType(new StructField[]{
                // StructField('id', IntegerType(), False)
                DataTypes.createStructField("id", DataTypes.IntegerType, false),

                // StructField('name', StringType(), True)
                DataTypes.createStructField("name", DataTypes.StringType, true),

                // StructField('age', IntegerType(), True)
                DataTypes.createStructField("age", DataTypes.IntegerType, true)
        });

        // 4. df = spark.createDataFrame(data, schema).show(truncate=False)
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        // Hiển thị kết quả, truncate=false được mặc định khi không truyền tham số
        // Để rõ ràng hơn, có thể dùng df.show(false);
        df.show(false);

        spark.stop();
    }
}
