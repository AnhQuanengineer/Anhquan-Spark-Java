package org.datapipeline.models.address;

import org.datapipeline.models.base.BaseEntity;

import java.io.Serializable;

public class Address extends BaseEntity implements Serializable {
    private String street;
    private String city;
    private String state;
    private String zipCode;
    private String country;

    // Constructors
    public Address() {
        super();
    }

    public Address(String id, String street, String city, String state, String zipCode, String country) {
        super(id);
        this.street = street;
        this.city = city;
        this.state = state;
        this.zipCode = zipCode;
        this.country = country;
    }

    // Getters and Setters
    public String getStreet() { return street; }
    public void setStreet(String street) { this.street = street; }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public String getZipCode() { return zipCode; }
    public void setZipCode(String zipCode) { this.zipCode = zipCode; }

    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }

    public String getFullAddress() {
        return String.format("%s, %s, %s %s, %s",
                street, city, state, zipCode, country);
    }

    @Override
    public String toString() {
        return "Address{" +
                "street='" + street + '\'' +
                ", city='" + city + '\'' +
                ", state='" + state + '\'' +
                ", zipCode='" + zipCode + '\'' +
                ", country='" + country + '\'' +
                "} " + super.toString();
    }
}
