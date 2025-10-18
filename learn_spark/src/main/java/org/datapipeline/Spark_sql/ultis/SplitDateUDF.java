package org.datapipeline.Spark_sql.ultis;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import java.util.regex.Pattern;

// UDF1<Kiểu dữ liệu đầu vào (String), Kiểu dữ liệu đầu ra (Row)>
public class SplitDateUDF implements UDF1<String, Row> {

    // Biểu thức chính quy tương đương với r"//|{}|[~*:./-]" trong Python
    // Lưu ý: Các ký tự như ., *, - cần được escape (\\) hoặc đặt trong [].
    // Dấu - nếu ở giữa [] là dải (range), nếu ở đầu/cuối là ký tự gạch ngang.
    private static final Pattern DATE_SPLIT_PATTERN = Pattern.compile("//|\\{}|[~*:./-]");

    @Override
    public Row call(String dateStr) throws Exception {
        if (dateStr == null) {
            return RowFactory.create(null, null, null);
        }

        String cleanedStr = dateStr;
        // 1. Xử lý ký tự '-' ở đầu chuỗi
        if (cleanedStr.startsWith("-")) {
            cleanedStr = cleanedStr.substring(1);
        }

        // 2. Tách chuỗi bằng Regex
        // Phương thức split() của Pattern trả về một mảng String
        String[] parts = DATE_SPLIT_PATTERN.split(cleanedStr);

        // 3. Trả về Row (StructType)
        if (parts.length == 3) {
            // parts[0] = day, parts[1] = month, parts[2] = year
            return RowFactory.create(parts[0], parts[1], parts[2]);
        } else {
            // Trả về null nếu không khớp 3 phần
            return RowFactory.create(null, null, null);
        }
    }
}
