package com.example.testspark;

import org.apache.spark.api.java.JavaSparkContext;

public class SparkStreamingExample {

    private final JavaSparkContext sc;

    public SparkStreamingExample(JavaSparkContext sc) {
        this.sc = sc;
    }
}
