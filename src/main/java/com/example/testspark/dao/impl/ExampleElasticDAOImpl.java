package com.example.testspark.dao.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.example.testspark.config.ElasticMainClient;
import com.example.testspark.dao.entity.ElasticExampleModel;
import com.example.testspark.dao.entity.ExampleModel;
import com.example.testspark.dao.interfaces.ExampleElasticDAO;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

public class ExampleElasticDAOImpl implements ExampleElasticDAO {

    private static final Logger logger = LoggerFactory.getLogger(ExampleElasticDAOImpl.class);

    private static final String INDEX = "my_example_model";

    private final ElasticMainClient elasticMainClient;

    public ExampleElasticDAOImpl(ElasticMainClient elasticMainClient){
        this.elasticMainClient = elasticMainClient;
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
        try (var restClient = elasticMainClient.getClient()) {
            var elasticsearchClient = getElasticsearchClient(restClient);
            elasticsearchClient.indices().create(c -> c.index(indexName));
            return true;
        }  catch (Exception e) {
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
        try (var restClient = elasticMainClient.getClient()) {
            var elasticsearchClient = getElasticsearchClient(restClient);
            var result = elasticsearchClient.indices().exists(ExistsRequest.of(e -> e.index(indexName)));
            return Optional.of(result.value());
        }  catch (Exception e) {
            logger.error("Exception when checking if index exists: {}, index: {}", e.getMessage(), indexName);
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @Override
    public void save(ElasticExampleModel exampleModel) {
        save(List.of(exampleModel));
    }

    @Override
    public void save(List<ElasticExampleModel> list) {
        try (var restClient = elasticMainClient.getClient()) {
            var elasticsearchClient = getElasticsearchClient(restClient);
            final var response = elasticsearchClient.bulk(builder -> {
                for (ElasticExampleModel entity : list) {
                    builder.index(INDEX)
                            .operations(ob -> {
                                if (entity.getId() != null) {
                                    ob.index(ib -> ib.document(entity).id(String.valueOf(entity.getId())));
                                } else {
                                    ob.index(ib -> ib.document(entity));
                                }
                                return ob;
                            });
                }
                return builder;
            });
            logger.info("Response error: {}", response.errors());
        }  catch (Exception e) {
            logger.error("An exception occurred while saving: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void update(ElasticExampleModel exampleModel) {
        try (var restClient = elasticMainClient.getClient()) {
            var elasticsearchClient = getElasticsearchClient(restClient);
            var updateRequest = new UpdateRequest.Builder<ElasticExampleModel, ElasticExampleModel>()
                    .index(INDEX)
                    .id(String.valueOf(exampleModel.getId()))
                    .doc(exampleModel)
                    .build();

            elasticsearchClient.update(updateRequest, ExampleModel.class);
        }  catch (Exception e) {
            logger.error("Exception at update: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void delete(Long exampleModelId) {
        try (var restClient = elasticMainClient.getClient()) {
            var elasticsearchClient = getElasticsearchClient(restClient);
            var deleteRequest = new DeleteRequest.Builder()
                    .index(INDEX)
                    .id(String.valueOf(exampleModelId))
                    .build();

            elasticsearchClient.delete(deleteRequest);
        }  catch (Exception e) {
            logger.error("Exception at delete by id: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    private ElasticsearchClient getElasticsearchClient(RestClient restClient){
        return new ElasticsearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper()));
    }
}
