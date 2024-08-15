package com.example.testspark.util;

import org.apache.spark.sql.Dataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShowDebugInfo {

    private static final Logger logger = LoggerFactory.getLogger(ShowDebugInfo.class);

    /**
     * Метод выодит в консоль количество разделов (количество разделов в фрейме данных), первую строку данныхб схему данных
     * @param df абор строго типизированных объектов JVM
     */
    public static void getPartitionAndSchemaInfo(Dataset<?> df) {
        logger.info("Partition count repartition: " + df.rdd().partitions().length);
        df.show(1);
        df.printSchema();
    }

    /**
     * Метод выодит в консоль количество разделов (количество разделов в фрейме данных), первую строку данныхб схему данных
     * @param df абор строго типизированных объектов JVM
     * @param showCount количество строк которое нужно вывести
     */
    public static void getPartitionAndSchemaInfo(Dataset<?> df, int showCount) {
        logger.info("Partition count repartition: " + df.rdd().partitions().length);
        df.show(showCount);
        df.printSchema();
    }

    /**
     * Метод выодит в консоль количество разделов (количество разделов в фрейме данных), первую строку данныхб схему данных
     * @param df абор строго типизированных объектов JVM
     * @param showCount количество строк которое нужно вывести
     * @param showSchema флаг нужно ли показать схему
     */
    public static void getPartitionAndSchemaInfo(Dataset<?> df, int showCount, boolean showSchema) {
        logger.info("Partition count repartition: " + df.rdd().partitions().length);
        df.show(showCount);
        if (showSchema) {
            df.printSchema();
        }
    }
}
