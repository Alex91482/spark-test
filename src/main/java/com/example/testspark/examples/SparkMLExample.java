package com.example.testspark.examples;

import com.example.testspark.common.udfs.ToVectorUdf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.dayofweek;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

public class SparkMLExample {

    public static final String Nb_UDF_LABEL_POINT = "toLabelPoint";

    public void execute() {
        String path = "./data/data.csv";

        SparkSession spark = SparkSession.builder()
                .appName("workload ml tree")
                .master("local[*]")
                .config("spark.driver.memory", "10g")
                .getOrCreate();

        spark.udf().register(Nb_UDF_LABEL_POINT, new ToVectorUdf(), SQLDataTypes.VectorType());

        Dataset<Row> df = spark
                .read()
                .format("csv")
                .option("header", true)
                .option("delimiter", "\t")
                .csv(path);

        df = df.withColumn("date1", to_date(col("date"), "dd.MM.yyyy"));
        df = df.withColumn("day", date_format(col("date1"), "d"))
                .withColumn("month", date_format(col("date1"), "M"))
                .withColumn("day_of_the_week", dayofweek(col("date1")))
        ;
        df =df.withColumn("hours_8-9", col("hours 8-9").cast("integer"))
                .withColumn("hours_9-10", col("hours 9-10").cast("integer"))
                .withColumn("hours_10-11", col("hours 10-11").cast("integer"))
                .withColumn("hours_11-12", col("hours 11-12").cast("integer"))
                .withColumn("hours_12-13", col("hours 12-13").cast("integer"))
                .withColumn("hours_13-14", col("hours 13-14").cast("integer"))
                .withColumn("hours_14-15", col("hours 14-15").cast("integer"))
                .withColumn("hours_15-16", col("hours 15-16").cast("integer"))
                .withColumn("hours_16-17", col("hours 16-17").cast("integer"))
                .withColumn("hours_17-18", col("hours 17-18").cast("integer"))
                .withColumn("hours_18-19", col("hours 18-19").cast("integer"))
                .withColumn("hours_19-20", col("hours 19-20").cast("integer"))
                .withColumn("hours_20-21", col("hours 20-21").cast("integer"))
                .withColumn("hours_21-22", col("hours 21-22").cast("integer"))
                .withColumn("hours_22-23", col("hours 22-23").cast("integer"))
                .withColumn("day_", col("day").cast("integer"))
                .withColumn("month_", col("month").cast("integer"))
        ;

        df = df.drop("date","date1","hours 8-9","hours 9-10","hours 10-11","hours 11-12","hours 12-13","hours 13-14",
                "hours 14-15","hours 15-16","hours 16-17","hours 17-18","hours 18-19","hours 19-20","hours 20-21","hours 21-22",
                "hours 22-23", "day","month"
        );

        var df1 = transform(df);
        var df2 = transformationDenseVector(df1);

        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(5)
                .fit(df2);

        //делим на два набора данных - тренировочный и тестовый
        Dataset<Row>[] dfArr = df2.randomSplit(new double[]{0.8, 0.2});
        var train = dfArr[0];
        var test = dfArr[1];

        DecisionTreeRegressor dt = new DecisionTreeRegressor()
                .setFeaturesCol("indexedFeatures");

        var pipeline = new Pipeline().setStages(new PipelineStage[]{featureIndexer, dt});
        var model = pipeline.fit(train);

        //получить предсказание
        var prediction = model.transform(test);
        prediction.show();
    }

    /**
     * Метод трансформирует данные путем переноса временого интервала в таблицу
     * И переименовывает кол-во посетителей в метку (label)
     * @param df набор данных который будет трансформирован
     * @return возвращает обновленный набор данных
     */
    private Dataset<Row> transform(Dataset<Row> df) {
        var arrColName = new ArrayList<String>(Arrays.asList("hours_8-9",
                "hours_9-10","hours_10-11","hours_11-12","hours_12-13","hours_13-14","hours_14-15", "hours_15-16",
                "hours_16-17","hours_17-18","hours_18-19","hours_19-20","hours_20-21","hours_21-22", "hours_22-23"
        ));

        var df1 = df.drop("hours_9-10","hours_10-11","hours_11-12","hours_12-13","hours_13-14",
                "hours_14-15", "hours_15-16","hours_16-17","hours_17-18","hours_18-19","hours_19-20","hours_20-21",
                "hours_21-22", "hours_22-23");

        df1 = df1.withColumn("time_interval_start", lit(8));
        df1 = df1.withColumn("time_interval_end", lit(9));
        df1 = df1.withColumnRenamed("hours_8-9", "label");

        for(int i = 1; i < arrColName.size(); i++) {
            var colName = arrColName.get(i);
            var intervalArr = colName.replace("hours_","").split("-");

            var cols = new ArrayList<String>(arrColName);
            cols.remove(colName);

            var df2 = df.drop(cols.toArray(new String[0]));
            df2 = df2.withColumn("time_interval_start", lit(Integer.parseInt(intervalArr[0])));
            df2 = df2.withColumn("time_interval_end", lit(Integer.parseInt(intervalArr[1])));
            df2 = df2.withColumnRenamed(colName, "label");

            df1 =df1.unionByName(df2);
        }

        for(String colN : df1.columns()) {
            df1 = df1.withColumn(colN + "_", col(colN).cast("double"));
            df1 = df1.drop(colN);
            df1 = df1.withColumnRenamed(colN + "_", colN);
        }

        return df1;
    }

    /**
     * Метод создает векор по 5 колонкам в формате double
     * @param df набор данных для которого требуется вычислить вектор
     * @return возвращает исходный набор данных с дополнительной колонкой в которой 5 колонкам создан вектор
     */
    private Dataset<Row> transformationDenseVector(Dataset<Row> df) {

        df = df.withColumn("features", callUDF(Nb_UDF_LABEL_POINT,
                col("day_of_the_week"),
                col("day_"),
                col("month_"),
                col("time_interval_start"),
                col("time_interval_end")
        ));
        return df;
    }
}
