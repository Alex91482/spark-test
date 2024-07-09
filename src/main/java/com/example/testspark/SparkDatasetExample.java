package com.example.testspark;

import com.example.testspark.dao.entity.ExampleModel;
import com.example.testspark.mappers.ExampleModelMapper;
import com.example.testspark.util.ShowDebugInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

public class SparkDatasetExample {

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
        Dataset<Row> df = dfData.drop("OKRYG_SV","SETKA","VESA_DX","VESA_SVOD","BB2","NAS_VOZ2","NAS_VOZ4",
                "STRUKTAK","Int8_E","C4_13","C4_1","C4_2","C4_3","C4_4","C4_11","C4_5","C4_7","C4_8","C4_14","C4_18",
                "C4_19","C4_6","C4_10","C4_17","Int3_1","Int3_2","Int3_3","Int3_4","Int3_13","Int3_10","Int3_12",
                "Int4_1","Int4_2","Int4_3","Int4_4","Int4_5","Int4_6","Int4_7","Int4_8","Int5_1","Int5_2","Int5_29","Int5_3","Int5_23",
                "Int5_4","Int5_5","Int5_6","Int5_7","Int5_8","Int5_9","Int5_10","Int5_24","Int5_25","Int5_12","Int5_13","Int5_14",
                "Int5_15","Int5_16","Int5_30","Int5_31","Int5_32","Int5_18","Int5_19","Int5_20","Int5_21","Int5_26","Int5_27","Int5_28",
                "Int5_22","Int6_1","Int6_3","Int6_10","Int6_11","Int6_12","Int6_5","Int6_7","Int6_8","Int7_1","Int7_2","Int7_3","Int7_4",
                "Int7_5","IntZ1","Int8_1_1","Int8_1_2","Int8_1_3","Int8_4","Int8_5","Int8_7","Int8_6","Int8_8","Int8_1_E","IntP1","IntP2_1",
                "IntP2_2","IntP2_18","IntP2_19","IntP2_4","IntP2_5","IntP2_17","IntP2_6","IntP2_7","IntP2_8","IntP2_9","IntP2_10","IntP2_11",
                "IntP2_12","IntP2_13","IntP2_14","IntP2_16","IntP2_20","IntP2_21","IntP2_15","IntP3_1","IntP3_11","IntP3_2","IntP3_3","IntP3_4",
                "IntP3_5","IntP3_6","IntP3_12","IntP3_7","IntP3_10","IntP4_1","IntP4_2","IntP4_3","IntP4_4","IntP4_5","IntP4_6","IntP4_7","IntP4_9",
                "IntP4_8","IntP5_1","IntP5_2","IntP5_3","PS1_1","PS1_4","PS1_5","PS1_6","PS1_7","PS1_E","PS3_1","PS3_2","PS3_3","PS3_4","PS3_5",
                "PS3_7","PS3_8","PS3_9","PS3_10","PS3_6","PS5_1","PS5_2","PS5_3","PS5_6","PS5_4","PS5_5","PS6","PS7_1","PS7_2","PS7_3","PS7_4",
                "PS7_5","PS7_6","PS7_8","PS7_9","PS7_10","PS8","PS9");


        var df_1 = df.filter(df.col("PS10").isNotNull()).persist();
        var df_max = df_1.agg(max(df_1.col("PS10")));
        var df_min = df_1.agg(min(df_1.col("PS10")));
        var df_group_by = df_1.drop("TERRIT","dx_unique","POSEL", "GOD","NAS_POL","NAS_VOZR","NASOBRAZ",
                        "C1","CInt1", "CInt3_1","CInt3_2","CInt3_3","CInt3_E","CInt5_1","CInt5_2","CInt5_8", "CInt5_9",
                        "CInt5_5","CInt5_6","CInt5_10","CInt5_7"," M1"," C2","Int1","Int2"
                )
                .groupBy(df_1.col("PS10"))
                .count()
                .sort(df_1.col("PS10"))
                ;

        System.out.println("Total number: " + df.count());
        System.out.println("Filtered quantity: " + df_1.count());
        ShowDebugInfo.getPartitionAndSchemaInfo(df_max);
        ShowDebugInfo.getPartitionAndSchemaInfo(df_min);
        ShowDebugInfo.getPartitionAndSchemaInfo(df_group_by, 10);

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
     * Загрузка json
     */
    public Dataset<Row> loadJsonFile() {
        SparkSession spark = SparkSession.builder()
                .appName("json read")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .json(JSON1_PATH);

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
