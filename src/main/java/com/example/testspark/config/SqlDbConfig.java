package com.example.testspark.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class SqlDbConfig {

    private Connection connection;
    private static SqlDbConfig sqlDbConfig;
    private final Properties properties;
    private final String DB_URI = "jdbc:postgresql://localhost/postgres";

    private SqlDbConfig() {
        this.properties = new Properties();
        this.properties.setProperty("driver", "org.postgresql.Driver");
        this.properties.setProperty("user", "user");
        this.properties.setProperty("password", "pgdb");
    }

    /**
     * Получить экземпляр класса с настройками для подключения к БД
     * @return возвращает экземпляр класса с настройками для подключения к БД
     */
    public static SqlDbConfig getSqlDbConfig() {
        if (sqlDbConfig == null) {
            sqlDbConfig = new SqlDbConfig();
        }
        return sqlDbConfig;
    }

    /**
     * Метод для получения экземпляра соединения с БД
     * @return возвращает кземпляра соединения с БД
     * @throws SQLException может выкинуть при подключении к базе
     */
    public Connection getPostgresConnection() throws SQLException {
        if (connection == null) {
            connection = DriverManager.getConnection(DB_URI, properties);
        }
        return connection;
    }

    /**
     * Метод закрывает соединение с БД
     * @param connection соединение с БД которое требуется закрыть
     */
    public void closeQuietly(Connection connection){
        try{
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Метод для поллучения параметров подключения к БД
     * @return возвращает экземпляр с настройками для подключения к БД
     */
    public Properties getProperties() {
        return this.properties;
    }

    /**
     * Метод получения uri для подключения к БД
     * @return возвращает адрес для подключения к БД
     */
    public String getDdUri() {
        return this.DB_URI;
    }
}
