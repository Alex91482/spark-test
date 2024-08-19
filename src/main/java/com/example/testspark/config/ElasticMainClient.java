package com.example.testspark.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticMainClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticMainClient.class);

    private static final String ELASTIC_API_ADDRESS = System.getProperty("db.elastic.address", "localhost");
    private static final Integer ELASTIC_API_PORT = Integer.parseInt(System.getProperty("db.elastic.port", "9200"));
    private static final String ELASTIC_USER = System.getProperty("db.elastic.user", "elastic");
    private static final String ELASTIC_PASSWORD = System.getProperty("db.elastic.pass", "password");
    private static final Boolean ELASTIC_AUTH_FLAG = Boolean.parseBoolean(System.getProperty("db.elastic.auth", "false"));

    private ElasticMainClient() {}

    private static ElasticMainClient instance;


    public static ElasticMainClient getInstance() {
        if (instance == null) {
            instance = new ElasticMainClient();
            logger.info("Create elastic client");
        }
        return instance;
    }

    public RestClient getClient() {
        if (ELASTIC_AUTH_FLAG) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ELASTIC_USER, ELASTIC_PASSWORD));
            RestClientBuilder builder = RestClient.builder(new HttpHost(ELASTIC_API_ADDRESS, ELASTIC_API_PORT, "HTTPS"))
                    .setHttpClientConfigCallback(
                            httpClientBuilder -> httpClientBuilder
                                    .setDefaultCredentialsProvider(credentialsProvider)
                                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                                    .setMaxConnTotal(20)
                                    .setMaxConnPerRoute(20)
                    );
            return builder.build();
        } else {
            return RestClient.builder(
                    new HttpHost(ELASTIC_API_ADDRESS, ELASTIC_API_PORT, "http")).build();
        }
    }
}
