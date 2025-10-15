package org.datapipeline.models.person;
import java.math.BigDecimal;
import java.time.LocalDate;

public class Customer extends Person {
    private String customerId;
    private BigDecimal totalSpent;
    private LocalDate registrationDate;
    private String loyaltyTier;

    // Constructors
    public Customer() {
        super();
    }

    public Customer(String id, String firstName, String lastName, String email,
                    LocalDate dateOfBirth, String customerId, BigDecimal totalSpent,
                    LocalDate registrationDate, String loyaltyTier) {
        super(id, firstName, lastName, email, dateOfBirth);
        this.customerId = customerId;
        this.totalSpent = totalSpent;
        this.registrationDate = registrationDate;
        this.loyaltyTier = loyaltyTier;
    }

    // Getters and Setters
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public BigDecimal getTotalSpent() { return totalSpent; }
    public void setTotalSpent(BigDecimal totalSpent) { this.totalSpent = totalSpent; }

    public LocalDate getRegistrationDate() { return registrationDate; }
    public void setRegistrationDate(LocalDate registrationDate) {
        this.registrationDate = registrationDate;
    }

    public String getLoyaltyTier() { return loyaltyTier; }
    public void setLoyaltyTier(String loyaltyTier) { this.loyaltyTier = loyaltyTier; }

    public boolean isVIP() {
        return "GOLD".equals(loyaltyTier) || "PLATINUM".equals(loyaltyTier);
    }

    @Override
    public String toString() {
        return "Customer{" +
                "customerId='" + customerId + '\'' +
                ", totalSpent=" + totalSpent +
                ", registrationDate=" + registrationDate +
                ", loyaltyTier='" + loyaltyTier + '\'' +
                ", isVIP=" + isVIP() +
                "} " + super.toString();
    }
}
