package com.example.testspark;

import com.example.testspark.util.ShowDebugInfo;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.callUDF;

public class SparkRddExample {

    private static final String README_PATH = "./README.md";
    private static final String CSV1_PATH = "./data/example_data.csv";
    private static final String JSON1_PATH = "./data/example_json_data.json";
    private static final String COLUMN_NAME_MD5 = "_md5";

    private final JavaSparkContext sc;

    public SparkRddExample(JavaSparkContext sc) {
        this.sc = sc;
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
     * Посчет слов в файле readme.md
     */
    public void readReadmeFile() {
        JavaRDD<String> lines = sc.textFile(README_PATH);
        JavaRDD<String> words = lines.map(l -> Arrays.asList(l.split(" "))).flatMap(List::iterator);
        JavaPairRDD<String, Integer> ones = words.map(String::toLowerCase).mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
    }

    /**
     * Загрузка csv файла в фрейм
     */
    public void loadCsvFile() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV read")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", true)
                .load(CSV1_PATH);

        df.show(2);
    }

    /**
     * Загрузка json
     */
    public void loadJsonFile() {
        SparkSession spark = SparkSession.builder()
                .appName("json read")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .json(JSON1_PATH);

        df.show(2);
    }
}
