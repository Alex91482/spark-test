package com.example.testspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TestSparkApplication {

    private static final String README_PATH = "./README.md";

    public static void main(String[]args) {

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount")
                .setMaster("local")
                .set("spark.executor.memory","1g");

        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {

            JavaRDD<String> lines = sc.textFile(README_PATH);
            JavaRDD<String> words = lines.map(l -> Arrays.asList(l.split(" "))).flatMap(List::iterator);
            JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
            JavaPairRDD<String, Integer> counts = ones.reduceByKey(Integer::sum);

            List<Tuple2<String, Integer>> output = counts.collect();
            for (Tuple2<?, ?> tuple : output) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }

            sc.stop();
        }
    }
}
