package com.example.testspark.config;

import com.clickhouse.client.ClickHouseNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ClickHouseDbConfig {

    private Connection connection;
    private ClickHouseNode server;
    private static ClickHouseDbConfig clickHouseDbConfig;
    private final Properties properties;
    private final String DB_URI = "jdbc:ch://localhost:8123/default";
    private final String DB_URI_VERSION_HTTP = "http://localhost:8123/default?compress=0";

    private ClickHouseDbConfig() {
        this.properties = new Properties();
        properties.put("user", "default");
        properties.put("password", "");
    }

    /**
     * Метод для получение экземпляра соединения с БД (для протакола http)
     * @return возвращает экземпляр ClickHouseNode
     */
    public ClickHouseNode getClickHouseNode() {
        if (server == null) {
            server = ClickHouseNode.of(DB_URI_VERSION_HTTP);
        }
        return server;
    }

    /**
     * Метод для получения экземпляра соединения с БД
     * @return возвращает кземпляра соединения с БД
     * @throws SQLException может выкинуть при подключении к базе
     */
    public Connection getClickHouseConnection() throws SQLException {
        if (connection == null) {
            connection = DriverManager.getConnection(DB_URI, properties);
        }
        return connection;
    }

    /**
     * Получить экземпляр класса с настройками для подключения к БД
     * @return возвращает экземпляр класса с настройками для подключения к БД
     */
    public static ClickHouseDbConfig getInstance() {
        if (clickHouseDbConfig == null) {
            clickHouseDbConfig = new ClickHouseDbConfig();
        }
        return clickHouseDbConfig;
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

    /**
     * Метод получения uri для подключения к БД по http
     * @return возвращает адрес для подключения к БД по http
     */
    public String getDbUriVersionHttp() {
        return this.DB_URI_VERSION_HTTP;
    }
}
