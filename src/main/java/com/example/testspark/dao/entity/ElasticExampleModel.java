package com.example.testspark.dao.entity;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;

public class ElasticExampleModel {

    private LocalDateTime registration_dttm; //format 2016-02-04T10:34:07Z
    private Long id;
    private String first_name;
    private String last_name;
    private String email;
    private String gender;
    private String ip_address;
    private Long cc;
    private String country;
    private LocalDate birthdate; //format 4/23/1980
    private Double salary;
    private String title;
    private String comments;

    public LocalDateTime getRegistration_dttm() {
        return registration_dttm;
    }

    public void setRegistration_dttm(LocalDateTime registration_dttm) {
        this.registration_dttm = registration_dttm;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFirst_name() {
        return first_name;
    }

    public void setFirst_name(String first_name) {
        this.first_name = first_name;
    }

    public String getLast_name() {
        return last_name;
    }

    public void setLast_name(String last_name) {
        this.last_name = last_name;
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

    public String getIp_address() {
        return ip_address;
    }

    public void setIp_address(String ip_address) {
        this.ip_address = ip_address;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ElasticExampleModel that = (ElasticExampleModel) o;

        if (!Objects.equals(registration_dttm, that.registration_dttm))
            return false;
        if (!Objects.equals(id, that.id)) return false;
        if (!Objects.equals(first_name, that.first_name)) return false;
        if (!Objects.equals(last_name, that.last_name)) return false;
        if (!Objects.equals(email, that.email)) return false;
        if (!Objects.equals(gender, that.gender)) return false;
        if (!Objects.equals(ip_address, that.ip_address)) return false;
        if (!Objects.equals(cc, that.cc)) return false;
        if (!Objects.equals(country, that.country)) return false;
        if (!Objects.equals(birthdate, that.birthdate)) return false;
        if (!Objects.equals(salary, that.salary)) return false;
        if (!Objects.equals(title, that.title)) return false;
        return Objects.equals(comments, that.comments);
    }

    @Override
    public int hashCode() {
        int result = registration_dttm != null ? registration_dttm.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (first_name != null ? first_name.hashCode() : 0);
        result = 31 * result + (last_name != null ? last_name.hashCode() : 0);
        result = 31 * result + (email != null ? email.hashCode() : 0);
        result = 31 * result + (gender != null ? gender.hashCode() : 0);
        result = 31 * result + (ip_address != null ? ip_address.hashCode() : 0);
        result = 31 * result + (cc != null ? cc.hashCode() : 0);
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + (birthdate != null ? birthdate.hashCode() : 0);
        result = 31 * result + (salary != null ? salary.hashCode() : 0);
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (comments != null ? comments.hashCode() : 0);
        return result;
    }
}
