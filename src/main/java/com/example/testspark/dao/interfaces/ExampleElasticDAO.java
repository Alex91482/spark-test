package com.example.testspark.dao.interfaces;

import java.util.Optional;

public interface ExampleElasticDAO {

    boolean createIndex(String indexName);
    Optional<Boolean> checkIndex(String indexName);
}
