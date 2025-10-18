package org.datapipeline.Spark_sql.Column;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.datapipeline.Spark_sql.ultis.SplitDateUDF;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class DateSplit {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Quan dz")
                .getOrCreate();

        // 1. Dữ liệu đầu vào
        List<Row> data = Arrays.asList(
                RowFactory.create("11//02/2025"),
                RowFactory.create("27/11-2021"),
                RowFactory.create("28.12-2005"),
                RowFactory.create("14~9*2002"),
                RowFactory.create("-31:03{}1995")
        );
        StructType inputSchema = new StructType(new StructField[]{
                new StructField("date", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, inputSchema);

        // 2. Định nghĩa StructType trả về của UDF
        StructType outputSchema = new StructType(new StructField[]{
                new StructField("dayas", DataTypes.StringType, true, Metadata.empty()),
                new StructField("monthas", DataTypes.StringType, true, Metadata.empty()),
                new StructField("yearas", DataTypes.StringType, true, Metadata.empty())
        });

        // 3. Đăng ký UDF
        spark.udf().register(
                "split_date",                     // Tên UDF
                new SplitDateUDF(),               // Instance của Class
                outputSchema                      // Kiểu dữ liệu trả về (StructType)
        );

        // 4. Áp dụng logic DataFrame
        Dataset<Row> dfWithSplitColumn = df.withColumn(
                "split_date",
                callUDF("split_date", col("date"))
        );

        // 5. Select và hiển thị (Flatten the StructType)
        dfWithSplitColumn.select(
                col("date").alias("date"),
                col("split_date.dayas").alias("day"),
                col("split_date.monthas").alias("month"),
                col("split_date.yearas").alias("year")
        ).show(false);

        spark.stop();
    }
}
