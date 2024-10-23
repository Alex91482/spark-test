package com.example.testspark.service.data_analysis;

import com.example.testspark.service.data_analysis.mapers.CharacterCountMapper;
import com.example.testspark.service.data_analysis.reducers.CharacterCountReducer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.util.List;

public class DataAnalyticsService {

    /**
     * Подсчет максимального количества символов в колонках в "плоских" файлах
     * @param dataset набор данных для подсчета
     * @param spark текущая сессия (нужна для создания еще оддного набора данных)
     * @param printSchema вывод в консоль схемы фрейма данных
     */
    public void countCsv(Dataset<Row> dataset, SparkSession spark, boolean printSchema) {

        Dataset<Row> df = getCountDataset(dataset, spark);

        df.show(1);
        if (printSchema) {
            df.printSchema();
        }
    }

    /**
     * Подсчет максимального количества символов в колонках в "плоских" файлах
     * @param dataset набор данных для подсчета
     * @param spark текущая сессия (нужна для создания еще оддного набора данных)
     * @return возвращает фрейм данных где в каждой колонкой подсчитано самое длиное слово  колонке
     */
    public Dataset<Row> getCountDataset(Dataset<Row> dataset, SparkSession spark) {
        Dataset<Row> df = dataset;

        var columnName = dataset.columns();
        var structField = new StructField[columnName.length];
        for (int i =0; i < structField.length; i++) {
            structField[i] = DataTypes.createStructField(columnName[i], DataTypes.IntegerType , true);
        }
        StructType schema = DataTypes.createStructType(structField);

        Row row = df
                .map(new CharacterCountMapper(), Encoders.row(schema))
                .reduce(new CharacterCountReducer());

        df = spark.createDataFrame(List.of(row), schema);
        return df;
    }

    public void countReduce(Dataset<Row> dataset, SparkSession spark) {
        var columnName = dataset.columns();
        var structField = new StructField[columnName.length];
        for (int i =0; i < structField.length; i++) {
            structField[i] = DataTypes.createStructField(columnName[i], DataTypes.IntegerType , true);
        }
        StructType schema = DataTypes.createStructType(structField);

        Row row = dataset.reduce(new CharacterCountReducer());
        var df = spark.createDataFrame(List.of(row), schema);

        df.show(1);
    }
}
