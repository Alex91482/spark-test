package com.example.testspark.dao.interfaces;

import com.example.testspark.dao.entity.ExampleModel;

import java.sql.SQLException;

public interface ExampleDAO {

    String getExampleSchemaName();
    String getExampleTableName();
    String getTableAndSchema();
    Long getExampleTableIsExist() throws SQLException;
    void createExampleSchema();
    void createExampleTable();
    void save(ExampleModel exampleModel) throws SQLException;
}
