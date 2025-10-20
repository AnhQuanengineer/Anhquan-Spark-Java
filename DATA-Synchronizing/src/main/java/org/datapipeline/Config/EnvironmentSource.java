package org.datapipeline.Config;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Triển khai ConfigurationSource, đọc cấu hình từ file .env (nếu có) và biến môi trường hệ thống.
 */
public class EnvironmentSource implements ConfigurationSource {

    // Sử dụng Dotenv để đọc file .env
    // Nếu bạn không muốn dùng thư viện: thay đổi thành private final Dotenv dotenv = null;
    private final Dotenv dotenv;

    public EnvironmentSource() {
        // Tải file .env từ thư mục gốc của dự án
        this.dotenv = Dotenv.load();
    }

    @Override
    public String getString(String key) {
        if (dotenv != null) {
            // Dotenv.get() sẽ kiểm tra cả file .env và System.getenv()
            return dotenv.get(key);
        }
        // Trường hợp không dùng thư viện dotenv:
        // return System.getenv(key);
        return null;
    }
}