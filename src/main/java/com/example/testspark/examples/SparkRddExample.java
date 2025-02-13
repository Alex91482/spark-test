package com.example.testspark.examples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkRddExample {

    private static final Logger logger = LoggerFactory.getLogger(SparkRddExample.class);

    private static final String README_PATH = "./README.md";

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
            logger.info(tuple._1() + ": " + tuple._2());
        }
    }
}
