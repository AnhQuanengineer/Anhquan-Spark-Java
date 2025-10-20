package org.datapipeline.Config;

/**
 * Interface để trừu tượng hóa việc đọc cấu hình.
 * Giúp mã ConfigLoader có thể kiểm thử và linh hoạt hơn.
 */
public interface ConfigurationSource {
    /**
     * Lấy giá trị chuỗi (String) tương ứng với key, trả về null nếu không tìm thấy.
     */
    String getString(String key);
}
