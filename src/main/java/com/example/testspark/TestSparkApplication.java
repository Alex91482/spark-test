package com.example.testspark;

import com.example.testspark.config.Init;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class TestSparkApplication {

    public static void main(String[]args) {

        Init.execute(); //создание схемы и таблиц

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount")
                .setMaster("local")
                .set("spark.executor.memory","1g");

        try (var sc = new JavaSparkContext(sparkConf)) {
            var rddExample = new SparkRddExample(sc);
            //rddExample.loadCsvFile();
            //rddExample.readReadmeFile();
            var sqlExample = new SparkSqlExample(sc);
            //sqlExample.readCsvAndSaveToDb();
        }

    }
}
