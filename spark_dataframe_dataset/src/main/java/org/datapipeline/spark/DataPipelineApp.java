package org.datapipeline.spark;
// Lưu ý: Đảm bảo package đã được đổi thành org.datapipeline

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.datapipeline.models.person.Customer;
import static org.apache.spark.sql.functions.*;

import java.math.BigDecimal;
import java.time.LocalDate;

public class DataPipelineApp {

    private static final String INPUT_JSON_PATH = "customers.json";
    private static final String OUTPUT_CSV_PATH = "output/processed_customers.csv";

    // -----------------------------------------------------------
    // KHAI BÁO SCHEMA THỦ CÔNG (StructType)
    // -----------------------------------------------------------

    // Định nghĩa Schema cho Address (StructType)
    private static final StructType ADDRESS_SCHEMA = DataTypes.createStructType(new StructField[]{
            // 1. Các trường kế thừa từ BaseEntity (LocalDateTime <-> TimestampType)
            DataTypes.createStructField("id", DataTypes.StringType, true),
            DataTypes.createStructField("active", DataTypes.BooleanType, true),
            DataTypes.createStructField("createdAt", DataTypes.TimestampType, true),
            DataTypes.createStructField("updatedAt", DataTypes.TimestampType, true),

            // 2. Các trường riêng của Address
            DataTypes.createStructField("street", DataTypes.StringType, true),
            DataTypes.createStructField("city", DataTypes.StringType, true),
            DataTypes.createStructField("state", DataTypes.StringType, true),
            DataTypes.createStructField("zipCode", DataTypes.StringType, true),
            DataTypes.createStructField("country", DataTypes.StringType, true)
    });

    // Định nghĩa Schema cho Customer (BigDecimal <-> DecimalType)
    private static final StructType CUSTOMER_SCHEMA = DataTypes.createStructType(new StructField[]{
            // 1. Các trường từ BaseEntity (cấp cao nhất)
            DataTypes.createStructField("id", DataTypes.StringType, true),
            DataTypes.createStructField("active", DataTypes.BooleanType, true),
            DataTypes.createStructField("createdAt", DataTypes.TimestampType, true),
            DataTypes.createStructField("updatedAt", DataTypes.TimestampType, true),

            // 2. Từ Person
            DataTypes.createStructField("firstName", DataTypes.StringType, true),
            DataTypes.createStructField("lastName", DataTypes.StringType, true),
            DataTypes.createStructField("email", DataTypes.StringType, true),
            DataTypes.createStructField("dateOfBirth", DataTypes.DateType, true),
            DataTypes.createStructField("address", ADDRESS_SCHEMA, true), // StructType lồng nhau

            // 3. Từ Customer
            DataTypes.createStructField("customerId", DataTypes.StringType, true),
            // Đã sửa: DoubleType -> DecimalType(38, 18) để khớp với java.math.BigDecimal
            DataTypes.createStructField("totalSpent", DataTypes.createDecimalType(38, 18), true),
            DataTypes.createStructField("registrationDate", DataTypes.DateType, true),
            DataTypes.createStructField("loyaltyTier", DataTypes.StringType, true)
    });

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("JSONDSTransformationPipeline")
                .master("local[*]")
                .getOrCreate();

        // -----------------------------------------------------------
        // BƯỚC 1: ĐỌC DỮ LIỆU TỪ JSON VÀO DATAFRAME
        // -----------------------------------------------------------
        System.out.println("--- BƯỚC 1: Đọc JSON vào DataFrame ---");

        Dataset<Row> initialDF = spark.read()
                .option("dateFormat", "yyyy-MM-dd")
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss") // Định dạng cho LocalDateTime
                .schema(CUSTOMER_SCHEMA)
                .json(INPUT_JSON_PATH);

        System.out.println("Schema (DF) đã được định nghĩa:");
        initialDF.printSchema();

        // -----------------------------------------------------------
        // BƯỚC 2: CHUYỂN SANG DATASET<CUSTOMER> VÀ XỬ LÝ NGHIỆP VỤ
        // -----------------------------------------------------------
        System.out.println("\n--- BƯỚC 2: Chuyển sang Dataset<Customer> và Xử lý Logic ---");

        // 2a. Chuyển đổi: DataFrame -> Dataset<Customer> (Đã khắc phục lỗi ánh xạ)
        Dataset<Customer> customerDS = initialDF.as(Encoders.bean(Customer.class));

        // 2b. Logic Xử lý nghiệp vụ (Cập nhật LoyaltyTier)
        Dataset<Customer> updatedDS = customerDS.map(
                (MapFunction<Customer, Customer>) customer -> {
                    BigDecimal total = customer.getTotalSpent();

                    if (total.compareTo(new BigDecimal("20000")) > 0) {
                        customer.setLoyaltyTier("PLATINUM");
                    } else if (total.compareTo(new BigDecimal("10000")) > 0) {
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
        // BƯỚC 3: CHUYỂN NGƯỢC LẠI DATAFRAME, LÀM PHẲNG VÀ GHI CSV
        // -----------------------------------------------------------
        System.out.println("\n--- BƯỚC 3: Làm phẳng DataFrame và Ghi CSV ---");

        Dataset<Row> resultDF = updatedDS.toDF();

        // Thêm cột is_VIP dựa trên loyaltyTier (Logic mới)
        Dataset<Row> dfWithVip = resultDF.withColumn(
                "is_VIP",
                when(col("loyaltyTier").equalTo("PLATINUM"), lit(true)).otherwise(lit(false))
        );

        // LÀM PHẲNG (Flatten) cột 'address' STRUCT để tương thích với CSV
        Dataset<Row> finalDF = dfWithVip.select(
                col("id").as("customer_id"), // Đổi tên để tránh nhầm lẫn với address_id
                col("firstName"),
                col("lastName"),
                col("email"),
                col("totalSpent"),
                col("loyaltyTier"),
                col("is_VIP"), // Cột mới

                // LÀM PHẲNG CÁC TRƯỜNG CỦA ADDRESS
                col("address.id").as("address_id"),
                col("address.street").as("street"),
                col("address.city").as("city"),
                col("address.state").as("state"),
                col("address.zipCode").as("zipCode"),
                col("address.country").as("country"),
                col("address.active").as("address_active"),
                col("address.updatedAt").as("address_updatedAt")
                // Loại bỏ address.createdAt và customerId để giữ output đơn giản
        );

        System.out.println("Schema cuối cùng (DF đã làm phẳng):");
        finalDF.printSchema();
        finalDF.show(false);

        // Ghi DataFrame đã làm phẳng ra CSV.
        finalDF.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(OUTPUT_CSV_PATH);

        System.out.println("\n--- PIPEINE HOÀN TẤT ---");
        System.out.println("Dữ liệu đã được ghi vào thư mục: " + OUTPUT_CSV_PATH);

        spark.stop();
    }
}
