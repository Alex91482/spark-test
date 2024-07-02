package com.example.testspark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkRddExample {

    private static final String README_PATH = "./README.md";
    private static final String CSV1_PATH = "./data/example_data.csv";

    private final JavaSparkContext sc;

    public SparkRddExample(JavaSparkContext sc) {
        this.sc = sc;
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
}
