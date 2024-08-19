package com.example.testspark.dao.entity;

import com.example.testspark.util.serializes.LocalDateDeserializer;
import com.example.testspark.util.serializes.LocalDateSerializer;
import com.example.testspark.util.serializes.LocalDateTimeSerializer;
import com.example.testspark.util.serializes.LocalDatetimeDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;

public class ElasticExampleModel {

    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @JsonDeserialize(using = LocalDatetimeDeserializer.class)
    private LocalDateTime registrationDttm;
    private Long id;
    private String firstName;
    private String lastName;
    private String email;
    private String gender;
    private String ipAddress;
    private Long cc;
    private String country;
    @JsonSerialize(using = LocalDateSerializer.class)
    @JsonDeserialize(using = LocalDateDeserializer.class)
    private LocalDate birthdate;
    private Double salary;
    private String title;
    private String comments;

    public LocalDateTime getRegistrationDttm() {
        return registrationDttm;
    }

    public void setRegistrationDttm(LocalDateTime registrationDttm) {
        this.registrationDttm = registrationDttm;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public Long getCc() {
        return cc;
    }

    public void setCc(Long cc) {
        this.cc = cc;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public LocalDate getBirthdate() {
        return birthdate;
    }

    public void setBirthdate(LocalDate birthdate) {
        this.birthdate = birthdate;
    }

    public Double getSalary() {
        return salary;
    }

    public void setSalary(Double salary) {
        this.salary = salary;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    @Override
    public String toString() {
        return "ElasticExampleModel{" +
                "registrationDttm=" + registrationDttm +
                ", id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", email='" + email + '\'' +
                ", gender='" + gender + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", cc=" + cc +
                ", country='" + country + '\'' +
                ", birthdate=" + birthdate +
                ", salary=" + salary +
                ", title='" + title + '\'' +
                ", comments='" + comments + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ElasticExampleModel that = (ElasticExampleModel) o;

        if (!Objects.equals(registrationDttm, that.registrationDttm))
            return false;
        if (!Objects.equals(id, that.id)) return false;
        if (!Objects.equals(firstName, that.firstName)) return false;
        if (!Objects.equals(lastName, that.lastName)) return false;
        if (!Objects.equals(email, that.email)) return false;
        if (!Objects.equals(gender, that.gender)) return false;
        if (!Objects.equals(ipAddress, that.ipAddress)) return false;
        if (!Objects.equals(cc, that.cc)) return false;
        if (!Objects.equals(country, that.country)) return false;
        if (!Objects.equals(birthdate, that.birthdate)) return false;
        if (!Objects.equals(salary, that.salary)) return false;
        if (!Objects.equals(title, that.title)) return false;
        return Objects.equals(comments, that.comments);
    }

    @Override
    public int hashCode() {
        int result = registrationDttm != null ? registrationDttm.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
        result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
        result = 31 * result + (email != null ? email.hashCode() : 0);
        result = 31 * result + (gender != null ? gender.hashCode() : 0);
        result = 31 * result + (ipAddress != null ? ipAddress.hashCode() : 0);
        result = 31 * result + (cc != null ? cc.hashCode() : 0);
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + (birthdate != null ? birthdate.hashCode() : 0);
        result = 31 * result + (salary != null ? salary.hashCode() : 0);
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (comments != null ? comments.hashCode() : 0);
        return result;
    }
}
