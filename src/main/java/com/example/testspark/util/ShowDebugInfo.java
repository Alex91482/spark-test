package com.example.testspark.util;

import org.apache.spark.sql.Dataset;

public class ShowDebugInfo {

    /**
     * Метод выодит в консоль количество разделов (количество разделов в фрейме данных), первую строку данныхб схему данных
     * @param df абор строго типизированных объектов JVM
     */
    public static void getPartitionAndSchemaInfo(Dataset<?> df) {
        System.out.println("Partition count repartition: " + df.rdd().partitions().length);
        df.show(1);
        df.printSchema();
    }
}