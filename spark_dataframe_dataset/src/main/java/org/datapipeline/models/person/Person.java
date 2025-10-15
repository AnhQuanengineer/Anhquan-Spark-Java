package org.datapipeline.models.person;

import org.datapipeline.models.address.Address;
import org.datapipeline.models.base.BaseEntity;

import java.io.Serializable;
import java.time.LocalDate;

public abstract class Person extends BaseEntity implements Serializable {
    private String firstName;
    private String lastName;
    private String email;
    private LocalDate dateOfBirth;
    private Address address;

    // Constructors
    public Person() {
        super();
    }

    public Person(String id, String firstName, String lastName, String email, LocalDate dateOfBirth) {
        super(id);
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.dateOfBirth = dateOfBirth;
    }

    // Getters and Setters
    public String getFirstName() { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }

    public String getLastName() { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }

    public String getFullName() {
        return firstName + " " + lastName;
    }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }

    public LocalDate getDateOfBirth() { return dateOfBirth; }
    public void setDateOfBirth(LocalDate dateOfBirth) { this.dateOfBirth = dateOfBirth; }

    public int getAge() {
        if (dateOfBirth == null) return 0;
        return LocalDate.now().getYear() - dateOfBirth.getYear();
    }

    public Address getAddress() { return address; }
    public void setAddress(Address address) { this.address = address; }

    public boolean isAdult() {
        return getAge() >= 18;
    }

    @Override
    public String toString() {
        return "Person{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", email='" + email + '\'' +
                ", dateOfBirth=" + dateOfBirth +
                ", age=" + getAge() +
                "} " + super.toString();
    }
}