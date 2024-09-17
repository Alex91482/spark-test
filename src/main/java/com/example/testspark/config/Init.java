package com.example.testspark.config;

import com.example.testspark.examples.DataConsumptionExample;
import com.example.testspark.examples.SparkElasticSearchExample;
import com.example.testspark.dao.impl.ExampleDAOImpl;
import com.example.testspark.dao.impl.ExampleElasticDAOImpl;
import com.example.testspark.dao.interfaces.ExampleDAO;
import com.example.testspark.dao.interfaces.ExampleElasticDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Init {

    private static final Logger logger = LoggerFactory.getLogger(Init.class);

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

    /**
     * Метод заполняет бд ElasticSearch данными из файла
     * @param dataConsumption ссылка на объект DataConsumptionExample
     * @param elasticSearchExample ссылка на объект SparkElasticSearchExample
     */
    public static void initDataElasticSearch(DataConsumptionExample dataConsumption, SparkElasticSearchExample elasticSearchExample) {
        if (dataConsumption == null || elasticSearchExample == null) {
            return;
        }
        var df = dataConsumption.getAvroFileData();
        var dfElasticExample = elasticSearchExample.getDatasetElasticExampleModel(df);
        var dfElasticCache = dfElasticExample.persist();
        var elasticClient = ElasticMainClient.getInstance();
        var exampleElasticDAO = new ExampleElasticDAOImpl(elasticClient);
        var exampleElasticList = dfElasticCache.collectAsList();
        var exampleModel = exampleElasticDAO.findById(exampleElasticList.get(0).getId());
        System.out.println(exampleModel);
        if (exampleModel == null) {
            exampleElasticDAO.save(dfElasticCache.collectAsList());
            logger.info("Fill elasticsearch with data. Number of data: {}", exampleElasticList.size());
        } else {
            logger.info("Elasticsearch the data already exists");
        }
    }
}
