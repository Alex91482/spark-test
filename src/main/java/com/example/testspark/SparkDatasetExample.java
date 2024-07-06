package com.example.testspark;

import com.example.testspark.dao.entity.ExampleModel;
import com.example.testspark.mappers.ExampleModelMapper;
import com.example.testspark.util.ShowDebugInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.callUDF;

public class SparkDatasetExample {

    private static final String CSV1_PATH = "./data/example_data.csv";
    private static final String JSON1_PATH = "./data/example_json_data.json";
    private static final String COLUMN_NAME_MD5 = "_md5";

    private final JavaSparkContext sc;

    public SparkDatasetExample(JavaSparkContext sc) {
        this.sc = sc;
    }

    /**
     * Преобразования набора данных в набор данных с pojo
     */
    public void createDatasetExampleModel() {
        Dataset<Row> df = loadCsvFile();

        ShowDebugInfo.getPartitionAndSchemaInfo(df);

        df = df.withColumn("md5", callUDF(COLUMN_NAME_MD5, df.col("guid")));
        Dataset<ExampleModel> exampleDs = df.map(new ExampleModelMapper(), Encoders.bean(ExampleModel.class));

        ShowDebugInfo.getPartitionAndSchemaInfo(exampleDs);
    }

    /**
     * Объединение двух наборов данных
     */
    public void joinDataCsvJson() {
        SparkSession spark = SparkSession.builder()
                .appName("union csv and json data")
                .master("local")
                .getOrCreate();
        Dataset<Row> dfCsv = spark.read()
                .format("csv")
                .option("header", true)
                .load(CSV1_PATH);
        dfCsv = dfCsv.withColumn("md5", callUDF(COLUMN_NAME_MD5, dfCsv.col("guid")));
        ShowDebugInfo.getPartitionAndSchemaInfo(dfCsv);

        Dataset<Row> dfJson = spark.read()
                .json(JSON1_PATH);
        dfJson = dfJson.withColumn("md5", callUDF(COLUMN_NAME_MD5, dfJson.col("guid")))
                .withColumnRenamed("index", "index_1")
                .withColumnRenamed("type", "type_1")
                .drop(dfJson.col("uuid"), dfJson.col("durationLength"), dfJson.col("root"));
        ShowDebugInfo.getPartitionAndSchemaInfo(dfJson);

        Dataset<Row> df = dfCsv.unionByName(dfJson); //объединение с учетом имен столбцов
        df.show(10);
    }

    /**
     * Загрузка csv файла в фрейм
     */
    public Dataset<Row> loadCsvFile() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV read")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", true)
                .load(CSV1_PATH);

        df.show(2);
        return df;
    }

    /**
     * Загрузка json
     */
    public Dataset<Row> loadJsonFile() {
        SparkSession spark = SparkSession.builder()
                .appName("json read")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .json(JSON1_PATH);

        df.show(2);
        return df;
    }

    /**
     * Метод создает из списка строк набор данных
     */
    public Dataset<String> createDatasetString() {
        SparkSession spark = SparkSession.builder()
                .appName("List to Dataset")
                .master("local")
                .getOrCreate();
        var list = Arrays.asList("one","two","tree","four");
        Dataset<String> ds = spark.createDataset(list, Encoders.STRING());
        ShowDebugInfo.getPartitionAndSchemaInfo(ds);
        return ds;
    }
}
