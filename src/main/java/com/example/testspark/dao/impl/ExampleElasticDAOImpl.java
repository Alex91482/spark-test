package com.example.testspark.dao.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.example.testspark.config.ElasticMainClient;
import com.example.testspark.dao.interfaces.ExampleElasticDAO;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Pattern;

public class ExampleElasticDAOImpl implements ExampleElasticDAO {

    private static final Logger logger = LoggerFactory.getLogger(ExampleElasticDAOImpl.class);

    private static final String INDEX = "my_example_model";

    private final ElasticsearchClient elasticsearchClient;

    public ExampleElasticDAOImpl(ElasticMainClient elasticMainClient){
        this.elasticsearchClient = getElasticsearchClient(elasticMainClient.getClient());
    }

    private ElasticsearchClient getElasticsearchClient(RestClient restClient){
        return new ElasticsearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
    }

    public String getIndexElasticsearch() {
        return INDEX;
    }

    /**
     * Создать раздел в бд, имя раздела должно быть маленькими буквами
     * @param indexName имя нового раздела
     * @return возвращает true если раздел создан, вернет false если по каким либо причинам раздел не может быть создан
     */
    @Override
    public boolean createIndex(String indexName) {
        if (Pattern.compile("[A-Z]+").matcher(indexName).find()) {
            logger.error("Index name must be to lower case");
            return false;
        }
        try{
            elasticsearchClient.indices().create(c -> c.index(indexName));
            return true;
        }catch (Exception e){
            logger.error("Exception at create index: {}, index: {}", e.getMessage(), indexName);
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Метод проверяет существует ли раздел в бд
     * @param indexName имя раздела для которого требуется проверка
     * @return возвращает Optional<Boolean> если обработка запроса призошла корректно, либо Optional.empty() при каком либо исключении
     */
    @Override
    public Optional<Boolean> checkIndex(String indexName) {
        try {
            var result = elasticsearchClient.indices().exists(ExistsRequest.of(e -> e.index(indexName)));
            return Optional.of(result.value());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Exception when checking if index exists: {}, index: {}", e.getMessage(), indexName);
        }
        return Optional.empty();
    }
}
