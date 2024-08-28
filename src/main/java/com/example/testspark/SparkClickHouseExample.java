package com.example.testspark;

import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkClickHouseExample {

    private static final Logger logger = LoggerFactory.getLogger(SparkClickHouseExample.class);

    private final JavaSparkContext sc;

    public SparkClickHouseExample(JavaSparkContext sc) {
        this.sc = sc;
    }

    /**
     * Запрос данных из clickhouse
     */
    public void getData() {}
}
