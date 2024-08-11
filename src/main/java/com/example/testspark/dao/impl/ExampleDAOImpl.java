package com.example.testspark.dao.impl;

import com.example.testspark.config.PostgresSqlDbConfig;
import com.example.testspark.dao.entity.ExampleModel;
import com.example.testspark.dao.interfaces.ExampleDAO;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ExampleDAOImpl implements ExampleDAO {

    private static final String EXAMPLE_SCHEMA = "example";
    private static final String EXAMPLE_TABLE = "example_table";

    @Override
    public void save(ExampleModel exampleModel) throws SQLException {
        Connection connection = PostgresSqlDbConfig.getSqlDbConfig().getPostgresConnection();
        String sql = "INSERT INTO " + EXAMPLE_TABLE + " (guid, title, index_1, date_added, last_modified, id, type_code, iconuri, type_1, uri, md5) " +
                "values (?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, exampleModel.getGuid());
        preparedStatement.setString(2, exampleModel.getTitle());
        preparedStatement.setInt(3, exampleModel.getIndex1());
        preparedStatement.setLong(4, exampleModel.getDateAdded());
        preparedStatement.setLong(5, exampleModel.getLastModified());
        preparedStatement.setInt(6, exampleModel.getId());
        preparedStatement.setInt(7, exampleModel.getTypeCode());
        preparedStatement.setString(8, exampleModel.getIconuri());
        preparedStatement.setString(9, exampleModel.getType1());
        preparedStatement.setString(10, exampleModel.getUri());
        preparedStatement.setString(11, exampleModel.getMd5());
        preparedStatement.executeUpdate();
    }

    @Override
    public Long getExampleTableIsExist() throws SQLException {
        Connection connection = PostgresSqlDbConfig.getSqlDbConfig().getPostgresConnection();
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '"+ EXAMPLE_TABLE +"';";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        return resultSet.getLong("count");
    }

    @Override
    public void createExampleSchema() {
        try {
            Connection connection = PostgresSqlDbConfig.getSqlDbConfig().getPostgresConnection();
            String sql = "CREATE SCHEMA IF NOT EXISTS " + EXAMPLE_SCHEMA + ";";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createExampleTable() {
        try {
            Connection connection = PostgresSqlDbConfig.getSqlDbConfig().getPostgresConnection();
            String sql = "CREATE TABLE IF NOT EXISTS " +
                    EXAMPLE_SCHEMA + "." + EXAMPLE_TABLE +
                    " (" +
                    "   guid VARCHAR(32) PRIMARY KEY," +
                    "   title VARCHAR(64)," +
                    "   index_1 INT," +
                    "   date_added BIGINT," +
                    "   last_modified BIGINT," +
                    "   id INT," +
                    "   type_code INT," +
                    "   iconuri VARCHAR(128)," +
                    "   type_1 VARCHAR(64)," +
                    "   uri VARCHAR(128)," +
                    "   md5 VARCHAR(32)" +
                    ");";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String getExampleSchemaName() {
        return EXAMPLE_SCHEMA;
    }

    public String getExampleTableName() {
        return EXAMPLE_TABLE;
    }

    public String getTableAndSchema() {
        return EXAMPLE_SCHEMA + "." + EXAMPLE_TABLE;
    }
}
