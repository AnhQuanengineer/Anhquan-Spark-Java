package org.datapipeline.Spark_sql.Column;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

public class Select {
    public static void main(String[] args) {
        // 1. Khởi tạo SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("ReadCsvWithDefinedSchema")
                .master("local[*]") // Chạy cục bộ
                .getOrCreate();

        // **QUAN TRỌNG:** Thay đổi đường dẫn này cho phù hợp với vị trí file CSV của bạn
        String csvFilePath = "people.csv";

        // 2. Định nghĩa Schema (Cấu trúc dữ liệu)
        StructType customSchema = DataTypes.createStructType(new StructField[]{
                // Tên cột phải khớp với header trong file CSV
                // Kiểu dữ liệu phải khớp với dữ liệu thực tế
                DataTypes.createStructField("id", DataTypes.IntegerType, false), // id: Integer, not nullable
                DataTypes.createStructField("name", DataTypes.StringType, true),  // name: String
                DataTypes.createStructField("age", DataTypes.IntegerType, true)   // age: Integer
        });

//        System.out.println("--- 1. Schema được định nghĩa ---");
//        System.out.println(customSchema.prettyJson());

        // 3. Đọc file CSV và áp dụng Schema
        Dataset<Row> peopleDf = spark.read()
                .option("header", "true")     // Báo cho Spark biết có dòng header để bỏ qua
                .option("inferSchema", "false") // BẮT BUỘC: Tắt tự động suy luận Schema
                .schema(customSchema)         // Áp dụng Schema đã định nghĩa
                .csv(csvFilePath);

//        // 4. Hiển thị Schema và Data
//        System.out.println("\n--- 2. Schema của DataFrame sau khi đọc ---");
//        peopleDf.printSchema();

//        System.out.println("\n--- 3. Dữ liệu DataFrame ---");

        peopleDf.show();

        Dataset<Row> df = peopleDf.select(
                col("age").divide(2).as("quan_dz")
                , upper(col("name")).as("victo")

        );

        df.show();

        spark.stop();
    }

}
