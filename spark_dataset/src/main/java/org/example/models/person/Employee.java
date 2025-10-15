package org.example.models.person;

import java.math.BigDecimal;
import java.time.LocalDate;

public class Employee extends Person {
    private String employeeId;
    private String department;
    private BigDecimal salary;
    private LocalDate hireDate;
    private String position;

    // Constructors
    public Employee() {
        super();
    }

    public Employee(String id, String firstName, String lastName, String email,
                    LocalDate dateOfBirth, String employeeId, String department,
                    BigDecimal salary, LocalDate hireDate, String position) {
        super(id, firstName, lastName, email, dateOfBirth);
        this.employeeId = employeeId;
        this.department = department;
        this.salary = salary;
        this.hireDate = hireDate;
        this.position = position;
    }

    // Getters and Setters
    public String getEmployeeId() { return employeeId; }
    public void setEmployeeId(String employeeId) { this.employeeId = employeeId; }

    public String getDepartment() { return department; }
    public void setDepartment(String department) { this.department = department; }

    public BigDecimal getSalary() { return salary; }
    public void setSalary(BigDecimal salary) { this.salary = salary; }

    public LocalDate getHireDate() { return hireDate; }
    public void setHireDate(LocalDate hireDate) { this.hireDate = hireDate; }

    public String getPosition() { return position; }
    public void setPosition(String position) { this.position = position; }

    public BigDecimal getYearsOfService() {
        if (hireDate == null) return BigDecimal.ZERO;
        return BigDecimal.valueOf(
                LocalDate.now().getYear() - hireDate.getYear()
        );
    }

    @Override
    public String toString() {
        return "Employee{" +
                "employeeId='" + employeeId + '\'' +
                ", department='" + department + '\'' +
                ", salary=" + salary +
                ", hireDate=" + hireDate +
                ", position='" + position + '\'' +
                ", yearsOfService=" + getYearsOfService() +
                "} " + super.toString();
    }
}
