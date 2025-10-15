package org.example.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.models.person.Customer;
import static org.apache.spark.sql.functions.*;

import java.math.BigDecimal;
import java.time.LocalDate;

public class DataPipelineApp {

    // CHỈNH SỬA ĐƯỜNG DẪN NÀY CHO PHÙ HỢP VỚI MÁY CỦA BẠN
    private static final String INPUT_CSV_PATH = "customers.csv";
    private static final String OUTPUT_CSV_PATH = "output/processed_customers.csv";

    // -----------------------------------------------------------
    // KHAI BÁO SCHEMA THỦ CÔNG (StructType)
    // Phải khớp với tên cột trong CSV và kiểu dữ liệu trong Customer.java
    // -----------------------------------------------------------
    private static final StructType CUSTOMER_SCHEMA = DataTypes.createStructType(new StructField[]{
            // Các trường kế thừa từ Person/BaseEntity
            DataTypes.createStructField("id", DataTypes.StringType, true),
            DataTypes.createStructField("firstName", DataTypes.StringType, true),
            DataTypes.createStructField("lastName", DataTypes.StringType, true),
            DataTypes.createStructField("email", DataTypes.StringType, true),
            DataTypes.createStructField("dateOfBirth", DataTypes.DateType, true),
            // Các trường của Customer
            DataTypes.createStructField("customerId", DataTypes.StringType, true),
            // totalSpent phải là DecimalType để khớp với BigDecimal trong Java
            DataTypes.createStructField("totalSpent", DataTypes.createDecimalType(10, 2), true),
            DataTypes.createStructField("registrationDate", DataTypes.DateType, true),
            DataTypes.createStructField("loyaltyTier", DataTypes.StringType, true)
    });

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("CSVDSTransformationPipelineWithSchema")
                .master("local[*]")
                .getOrCreate();

        // -----------------------------------------------------------
        // BƯỚC 1: ĐỌC DỮ LIỆU TỪ CSV VÀO DATAFRAME (Dùng Schema Thủ công)
        // -----------------------------------------------------------
        System.out.println("--- BƯỚC 1: Đọc CSV vào DataFrame (Dùng StructType) ---");

        Dataset<Row> initialDF = spark.read()
                .option("header", "true")
                .option("dateFormat", "yyyy-MM-dd")
                // SỬ DỤNG SCHEMA THỦ CÔNG
                .schema(CUSTOMER_SCHEMA)
                .csv(INPUT_CSV_PATH);

        System.out.println("Schema (DF) đã được định nghĩa:");
        initialDF.printSchema();
        initialDF.show(false);

        // -----------------------------------------------------------
        // BƯỚC 2: CHUYỂN SANG DATASET<CUSTOMER> VÀ XỬ LÝ NGHIỆP VỤ
        // -----------------------------------------------------------
        System.out.println("\n--- BƯỚC 2: Chuyển sang Dataset<Customer> và Xử lý Logic ---");

        // 2a. Chuyển đổi: DataFrame -> Dataset<Customer>
        // Việc định nghĩa schema ở trên đảm bảo kiểu dữ liệu khớp với Customer.java (Decimal/Date)
        Dataset<Customer> customerDS = initialDF.as(Encoders.bean(Customer.class));

        // 2b. Áp dụng Logic nghiệp vụ (cập nhật Loyalty Tier)
        Dataset<Customer> updatedDS = customerDS.map(
                (MapFunction<Customer, Customer>) customer -> {
                    if (customer.getTotalSpent().compareTo(new BigDecimal("20000")) > 0) {
                        customer.setLoyaltyTier("PLATINUM");
                    } else if (customer.getTotalSpent().compareTo(new BigDecimal("10000")) > 0) {
                        customer.setLoyaltyTier("GOLD");
                    } else {
                        customer.setLoyaltyTier("SILVER");
                    }
                    return customer;
                },
                Encoders.bean(Customer.class)
        );

        updatedDS.show(false);

        // -----------------------------------------------------------
        // BƯỚC 3: CHUYỂN NGƯỢC LẠI DATAFRAME VÀ GHI CSV
        // -----------------------------------------------------------
        System.out.println("\n--- BƯỚC 3: Chuyển lại DataFrame và Ghi CSV ---");

        Dataset<Row> resultDF = updatedDS.toDF();

        // Thêm cột isVIP bằng cách gọi phương thức isVIP()
        Dataset<Row> finalDF = resultDF.select(
                col("fullName"),
                col("totalSpent"),
                col("loyaltyTier"),
                col("VIP").as("is_VIP")
        );

        System.out.println("Schema cuối cùng (DF):");
        finalDF.printSchema();
        finalDF.show();

        // Ghi DataFrame ra CSV
        finalDF.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(OUTPUT_CSV_PATH);

        System.out.println("\n--- PIPEINE HOÀN TẤT ---");
        System.out.println("Dữ liệu đã được ghi vào thư mục: " + OUTPUT_CSV_PATH);

        spark.stop();
    }
}