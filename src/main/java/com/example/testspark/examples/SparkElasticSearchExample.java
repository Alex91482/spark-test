package com.example.testspark.examples;

import com.example.testspark.config.ElasticMainClient;
import com.example.testspark.dao.entity.ElasticExampleModel;
import com.example.testspark.dao.impl.ExampleElasticDAOImpl;
import com.example.testspark.dao.interfaces.ExampleElasticDAO;
import com.example.testspark.service.data_analysis.ShowDebugInfo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class SparkElasticSearchExample {

    private static final DateTimeFormatter formatterDate = DateTimeFormatter.ofPattern("M/d/yyyy");
    private static final DateTimeFormatter formatterDateTime = DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault());

    private final ElasticMainClient elasticMainClient;
    private final ExampleElasticDAO exampleElasticDAO;
    private final JavaSparkContext sc;

    public SparkElasticSearchExample(JavaSparkContext sc) {
        this.sc = sc;
        this.elasticMainClient = ElasticMainClient.getInstance();
        this.exampleElasticDAO = new ExampleElasticDAOImpl(elasticMainClient);
    }

    /**
     * Запрос данных из elasticsearch
     */
    public void getData() {
        SparkSession spark = SparkSession.builder()
                .appName("Data to ElasticSearch")
                .master("local")
                .config("spark.es.nodes","127.0.0.1")
                .config("spark.es.port","9200")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("org.elasticsearch.spark.sql")
                .option("es.mapping.date.rich", "false")
                .option("es.nodes.wan.only", "true")
                .option("es.nodes", "127.0.0.1")
                .option("es.port", "9200")
                .option("es.query", "?q=*")
                .load(exampleElasticDAO.getIndexElasticsearch());

        ShowDebugInfo.getPartitionAndSchemaInfo(df, 10);
    }

    /**
     * Метод заполняет бд тестовыми данными
     * @param exampleModelDataset набор данных ElasticExampleModel который требуется сохранить в бд
     */
    public void saveExampleDataToDB(Dataset<ElasticExampleModel> exampleModelDataset) {
        SparkSession spark = SparkSession.builder()
                .appName("Data to ElasticSearch")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = exampleModelDataset.toDF();
        JavaRDD<Row> rdd = df.javaRDD();
        JavaEsSpark.saveToEs(rdd, exampleElasticDAO.getIndexElasticsearch());
    }

    /**
     * Метод правращает набор данных row в pojo
     * @param dataset набор данных полученных из файла
     * @return возвращает набор из pojo
     */
    public Dataset<ElasticExampleModel> getDatasetElasticExampleModel(Dataset<Row> dataset) {
        return dataset.map((MapFunction<Row, ElasticExampleModel>) model -> {
            var elModel = new ElasticExampleModel();
            elModel.setId(model.getAs("id"));
            elModel.setFirstName(model.getAs("first_name"));
            elModel.setLastName(model.getAs("last_name"));
            elModel.setEmail(model.getAs("email"));
            elModel.setGender(model.getAs("gender"));
            elModel.setIpAddress(model.getAs("ip_address"));
            elModel.setCc(model.getAs("cc"));
            elModel.setCountry(model.getAs("country"));
            elModel.setSalary(model.getAs("salary"));
            elModel.setTitle(model.getAs("title"));
            elModel.setComments(model.getAs("comments"));

            String registrationDttm = model.getAs("registration_dttm");
            String birthdate = model.getAs("birthdate");
            if (registrationDttm != null && !registrationDttm.trim().isEmpty()) {
                try {
                    elModel.setRegistrationDttm(LocalDateTime.parse(registrationDttm, formatterDateTime));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (birthdate != null && !birthdate.trim().isEmpty()){
                try {
                    elModel.setBirthdate(LocalDate.parse(birthdate, formatterDate));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            return elModel;
            }, Encoders.bean(ElasticExampleModel.class));
    }
}
