package org.example.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.example.models.person.Employee;
import org.example.models.person.Customer;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

public class SparkApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("InheritanceDatasetExample")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();

        // Tạo Employees Dataset
        List<Employee> employees = Arrays.asList(
                new Employee("emp1", "John", "Doe", "john@company.com",
                        LocalDate.of(1990, 5, 15), "E001", "Engineering",
                        new BigDecimal("75000"), LocalDate.of(2020, 1, 10), "Senior Developer"),
                new Employee("emp2", "Jane", "Smith", "jane@company.com",
                        LocalDate.of(1985, 8, 22), "E002", "Marketing",
                        new BigDecimal("65000"), LocalDate.of(2019, 3, 5), "Manager")
        );

        Dataset<Employee> employeeDS = spark.createDataset(employees, Encoders.bean(Employee.class));

        // Tạo Customers Dataset
        List<Customer> customers = Arrays.asList(
                new Customer("cust1", "Alice", "Johnson", "alice@email.com",
                        LocalDate.of(1995, 2, 10), "C001", new BigDecimal("15000"),
                        LocalDate.of(2021, 6, 20), "GOLD"),
                new Customer("cust2", "Bob", "Wilson", "bob@email.com",
                        LocalDate.of(1988, 11, 30), "C002", new BigDecimal("5000"),
                        LocalDate.of(2022, 9, 15), "SILVER")
        );

        Dataset<Customer> customerDS = spark.createDataset(customers, Encoders.bean(Customer.class));

        // Operations trên Employee Dataset
        System.out.println("=== EMPLOYEE DATASET ===");
        employeeDS.filter((Employee emp) -> emp.getYearsOfService().compareTo(BigDecimal.valueOf(2)) > 0)
                .select("fullName", "department", "salary", "yearsOfService")
                .show();

//        employeeDS.filter(col("yearsOfService").gt(2))
//                .select("fullName", "department", "salary", "yearsOfService")
//                .show();

        // Operations trên Customer Dataset
        System.out.println("=== CUSTOMER DATASET ===");
        customerDS.filter(Customer::isVIP)
                .select("fullName", "totalSpent", "loyaltyTier")
                .show();

        // Convert to DataFrame cho SQL operations
        employeeDS.createOrReplaceTempView("employees");
        customerDS.createOrReplaceTempView("customers");

//        // SQL Query trên cả hai
//        spark.sql("SELECT 'Employee' as type, fullName, department as category, salary as amount " +
//                        "FROM employees " +
//                        "UNION ALL " +
//                        "SELECT 'Customer' as type, fullName, loyaltyTier as category, totalSpent as amount " +
//                        "FROM customers")
//                .show();

         //Type-safe transformations
        Dataset<Row> seniorEmployees = employeeDS
                // Filter type-safe
                .filter((FilterFunction<Employee>) emp -> emp.getSalary().compareTo(new BigDecimal("70000")) > 0)
                // Select các cột mong muốn. Spark sẽ dùng các getter methods
                .select(
                        col("fullName").as("name"), // fullName là một getter trong Person
                        col("department"),          // department là một getter trong Employee
                        col("salary"),              // salary là một getter trong Employee
                        col("age"),                 // age là một getter trong Person
                        col("active")               // isActive() là một getter trong BaseEntity
                );

        seniorEmployees.show();

        spark.stop();
    }
}
