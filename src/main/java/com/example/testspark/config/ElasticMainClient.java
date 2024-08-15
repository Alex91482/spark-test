package com.example.testspark.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticMainClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticMainClient.class);

    private static final String ELASTIC_API_ADDRESS = System.getProperty("db.elastic.address", "localhost");
    private static final Integer ELASTIC_API_PORT = Integer.parseInt(System.getProperty("db.elastic.port", "9200"));


    private static ElasticMainClient instance;


    public static ElasticMainClient getInstance() {
        if (instance == null) {
            instance = new ElasticMainClient();
            logger.info("Create elastic client");
        }
        return instance;
    }

    public RestClient getClient(){
        return RestClient.builder(
                new HttpHost(ELASTIC_API_ADDRESS, ELASTIC_API_PORT, "http")).build();
    }
}
