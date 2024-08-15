package com.example.testspark;

import com.example.testspark.dao.entity.ExampleModel;
import com.example.testspark.mappers.ExampleModelMapper;
import com.example.testspark.util.ShowDebugInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

public class SparkDatasetExample {

    private static final Logger logger = LoggerFactory.getLogger(SparkDatasetExample.class);

    private static final String CSV1_PATH = "./data/example_data.csv";
    private static final String JSON1_PATH = "./data/example_json_data.json";
    private static final String OPEN_DATA = "./data/open_data/data-20240410-structure-20240410.csv";
    private static final String OPEN_DATA_STRUCTURE = "./data/open_data/structure-20240410.csv";
    private static final String COLUMN_NAME_MD5 = "_md5";

    private final JavaSparkContext sc;

    public SparkDatasetExample(JavaSparkContext sc) {
        this.sc = sc;
    }

    /**
     * Вычисления максимальной и минимальной оценки, подсчет оценок и прочее
     */
    public void conversionOperations() {
        SparkSession spark = SparkSession.builder()
                .appName("transformation data")
                .master("local")
                .getOrCreate();
        Dataset<Row> dfData = spark.read()
                .format("csv")
                .option("header", true)
                .option("delimiter", ";")
                .csv(OPEN_DATA);
        Dataset<Row> dfStruct = spark.read()
                .format("csv")
                .option("header", true)
                .option("delimiter", ";")
                .csv(OPEN_DATA_STRUCTURE);
        Dataset<Row> df = dfData.select("Household_ID","Respondent_number","TERRIT","dx_unique","POSEL","GOD",
                "NAS_POL","NAS_VOZR","NASOBRAZ", "C1","CInt1","CInt2_1","CInt2_2","CInt2_3","CInt2_8","CInt2_9","CInt2_5",
                "CInt2_6","CInt2_7","CInt3_1","CInt3_2", "CInt3_3","CInt3_E","CInt5_1","CInt5_2","CInt5_8","CInt5_9",
                "CInt5_5","CInt5_6","CInt5_10","CInt5_7","M1","C2", "Int1","Int2","PS10"
        );

        var df_1 = df.filter(df.col("PS10").isNotNull()).persist();
        var df_max = df_1.agg(max(df_1.col("PS10")));
        var df_min = df_1.agg(min(df_1.col("PS10")));
        var df_group_by = df_1.drop("TERRIT","dx_unique","POSEL", "GOD","NAS_POL","NAS_VOZR","NASOBRAZ",
                        "C1","CInt1", "CInt3_1","CInt3_2","CInt3_3","CInt3_E","CInt5_1","CInt5_2","CInt5_8", "CInt5_9",
                        "CInt5_5","CInt5_6","CInt5_10","CInt5_7"," M1"," C2","Int1","Int2"
                )
                .groupBy(df_1.col("PS10"))
                .count()
                .sort(df_1.col("PS10"));

        var zero = lit(0);
        var df_expr_sum = df_1
                .drop("C1","CInt1","CInt2_7", "CInt3_1","CInt3_2","CInt3_3","CInt3_E","CInt5_1","CInt5_2","CInt5_8", "CInt5_9",
                        "CInt5_5","CInt5_6","CInt5_10","CInt5_7"," M1"," C2","Int1","Int2")
                .withColumn("c2_1", coalesce(col("CInt2_1"), zero))
                .withColumn("c2_2", coalesce(col("CInt2_2"), zero))
                .withColumn("c2_3", coalesce(col("CInt2_3"), zero))
                .withColumn("c2_5", coalesce(col("CInt2_5"), zero))
                .withColumn("c2_6", coalesce(col("CInt2_6"), zero))
                .withColumn("c2_8", coalesce(col("CInt2_8"), zero))
                .withColumn("c2_9", coalesce(col("CInt2_9"), zero))
                .drop("CInt2_1","CInt2_2","CInt2_3","CInt2_5","CInt2_6","CInt2_8","CInt2_9")
                .withColumnRenamed("c2_1", "CInt2_1")
                .withColumnRenamed("c2_2", "CInt2_2")
                .withColumnRenamed("c2_3", "CInt2_3")
                .withColumnRenamed("c2_5", "CInt2_5")
                .withColumnRenamed("c2_6", "CInt2_6")
                .withColumnRenamed("c2_8", "CInt2_8")
                .withColumnRenamed("c2_9", "CInt2_9")
                .withColumn("number_of_devices", expr("(CInt2_1 + CInt2_2 + CInt2_3 + CInt2_5 + CInt2_6 + CInt2_8 + CInt2_9)"));

        logger.info("Total number: " + df.count());
        logger.info("Filtered quantity: " + df_1.count());
        ShowDebugInfo.getPartitionAndSchemaInfo(df_max);
        ShowDebugInfo.getPartitionAndSchemaInfo(df_min);
        ShowDebugInfo.getPartitionAndSchemaInfo(df_group_by, 10, false);
        ShowDebugInfo.getPartitionAndSchemaInfo(df_expr_sum, 10, false);

    }

    /**
     * Преобразования набора данных в набор данных с pojo
     */
    public void createDatasetExampleModel() {
        Dataset<Row> df = loadCsvFile();

        ShowDebugInfo.getPartitionAndSchemaInfo(df);

        df = df.withColumn("md5", callUDF(COLUMN_NAME_MD5, df.col("guid")));
        Dataset<ExampleModel> exampleDs = df.map(new ExampleModelMapper(), Encoders.bean(ExampleModel.class));

        ShowDebugInfo.getPartitionAndSchemaInfo(exampleDs);
    }

    /**
     * Объединение двух наборов данных
     */
    public void joinDataCsvJson() {
        SparkSession spark = SparkSession.builder()
                .appName("union csv and json data")
                .master("local")
                .getOrCreate();
        Dataset<Row> dfCsv = spark.read()
                .format("csv")
                .option("header", true)
                .load(CSV1_PATH);
        dfCsv = dfCsv.withColumn("md5", callUDF(COLUMN_NAME_MD5, dfCsv.col("guid")));
        ShowDebugInfo.getPartitionAndSchemaInfo(dfCsv);

        Dataset<Row> dfJson = spark.read()
                .json(JSON1_PATH);
        dfJson = dfJson.withColumn("md5", callUDF(COLUMN_NAME_MD5, dfJson.col("guid")))
                .withColumnRenamed("index", "index_1")
                .withColumnRenamed("type", "type_1")
                .drop(dfJson.col("uuid"), dfJson.col("durationLength"), dfJson.col("root"));
        ShowDebugInfo.getPartitionAndSchemaInfo(dfJson);

        Dataset<Row> df = dfCsv.unionByName(dfJson); //объединение с учетом имен столбцов
        df.show(10);
    }

    /**
     * Загрузка csv файла в фрейм
     */
    public Dataset<Row> loadCsvFile() {
        SparkSession spark = SparkSession.builder()
                .appName("CSV read")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", true)
                .load(CSV1_PATH);

        df.show(2);
        return df;
    }

    /**
     * Метод создает из списка строк набор данных
     */
    public Dataset<String> createDatasetString() {
        SparkSession spark = SparkSession.builder()
                .appName("List to Dataset")
                .master("local")
                .getOrCreate();
        var list = Arrays.asList("one","two","tree","four");
        Dataset<String> ds = spark.createDataset(list, Encoders.STRING());
        ShowDebugInfo.getPartitionAndSchemaInfo(ds);
        return ds;
    }
}
