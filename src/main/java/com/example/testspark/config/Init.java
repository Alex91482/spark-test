package com.example.testspark.config;

import com.example.testspark.dao.impl.ExampleDAOImpl;
import com.example.testspark.dao.impl.ExampleElasticDAOImpl;
import com.example.testspark.dao.interfaces.ExampleDAO;
import com.example.testspark.dao.interfaces.ExampleElasticDAO;

public class Init {

    private Init(){}

    /**
     * Метод выполняет проверку что таблица example_table существует
     * Если таблица существует то exampleDAO.getExampleTableIsExist() вернет 1
     * Если вернет 0 то создаем схему и таблице
     */
    public static void execute() {
        ExampleDAO exampleDAO = new ExampleDAOImpl();
        try {
            if (exampleDAO.getExampleTableIsExist() > 0) {
                return;
            }
            exampleDAO.createExampleSchema();
            exampleDAO.createExampleTable();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Метод проверяет существование раздела my_example_model
     * Если раздела не существует то он будет создан
     */
    public static void executeElastic() {
        var elasticClient = ElasticMainClient.getInstance();
        ExampleElasticDAO exampleElasticDAO = new ExampleElasticDAOImpl(elasticClient);
        try {
            var indexName = exampleElasticDAO.getIndexElasticsearch();
            var checkIndex = exampleElasticDAO.checkIndex(indexName);
            if (checkIndex.isPresent() && checkIndex.get()) {
                return;
            }
            boolean result = exampleElasticDAO.createIndex(indexName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
