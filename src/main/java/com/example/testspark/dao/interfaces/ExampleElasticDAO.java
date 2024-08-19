package com.example.testspark.dao.interfaces;

import com.example.testspark.dao.entity.ElasticExampleModel;

import java.util.List;
import java.util.Optional;

public interface ExampleElasticDAO {

    String getIndexElasticsearch();
    boolean createIndex(String indexName);
    Optional<Boolean> checkIndex(String indexName);
    void save(ElasticExampleModel exampleModel);
    void save(List<ElasticExampleModel> list);
    void update(ElasticExampleModel exampleModel);
    void delete(Long exampleModelId);
    ElasticExampleModel findById(Long id);
}
