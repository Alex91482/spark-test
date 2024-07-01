package com.example.testspark.dao.entity;

import java.util.Objects;

public class ExampleModel {

    private String guid;
    private String title;
    private Integer index1;
    private Long dateAdded;
    private Long lastModified;
    private Integer id;
    private Integer typeCode;
    private String iconuri;
    private String type1;
    private String uri;
    private String md5;

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Integer getIndex1() {
        return index1;
    }

    public void setIndex1(Integer index1) {
        this.index1 = index1;
    }

    public Long getDateAdded() {
        return dateAdded;
    }

    public void setDateAdded(Long dateAdded) {
        this.dateAdded = dateAdded;
    }

    public Long getLastModified() {
        return lastModified;
    }

    public void setLastModified(Long lastModified) {
        this.lastModified = lastModified;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getTypeCode() {
        return typeCode;
    }

    public void setTypeCode(Integer typeCode) {
        this.typeCode = typeCode;
    }

    public String getIconuri() {
        return iconuri;
    }

    public void setIconuri(String iconuri) {
        this.iconuri = iconuri;
    }

    public String getType1() {
        return type1;
    }

    public void setType1(String type1) {
        this.type1 = type1;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExampleModel that = (ExampleModel) o;

        if (!Objects.equals(guid, that.guid)) return false;
        if (!Objects.equals(title, that.title)) return false;
        if (!Objects.equals(index1, that.index1)) return false;
        if (!Objects.equals(dateAdded, that.dateAdded)) return false;
        if (!Objects.equals(lastModified, that.lastModified)) return false;
        if (!Objects.equals(id, that.id)) return false;
        if (!Objects.equals(typeCode, that.typeCode)) return false;
        if (!Objects.equals(iconuri, that.iconuri)) return false;
        if (!Objects.equals(type1, that.type1)) return false;
        if (!Objects.equals(uri, that.uri)) return false;
        return Objects.equals(md5, that.md5);
    }

    @Override
    public int hashCode() {
        int result = guid != null ? guid.hashCode() : 0;
        result = 31 * result + (title != null ? title.hashCode() : 0);
        result = 31 * result + (index1 != null ? index1.hashCode() : 0);
        result = 31 * result + (dateAdded != null ? dateAdded.hashCode() : 0);
        result = 31 * result + (lastModified != null ? lastModified.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (typeCode != null ? typeCode.hashCode() : 0);
        result = 31 * result + (iconuri != null ? iconuri.hashCode() : 0);
        result = 31 * result + (type1 != null ? type1.hashCode() : 0);
        result = 31 * result + (uri != null ? uri.hashCode() : 0);
        result = 31 * result + (md5 != null ? md5.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ExampleModel{" +
                "guid='" + guid + '\'' +
                ", title='" + title + '\'' +
                ", index1=" + index1 +
                ", dateAdded=" + dateAdded +
                ", lastModified=" + lastModified +
                ", id=" + id +
                ", typeCode=" + typeCode +
                ", iconuri='" + iconuri + '\'' +
                ", type1='" + type1 + '\'' +
                ", uri='" + uri + '\'' +
                ", md5='" + md5 + '\'' +
                '}';
    }
}
