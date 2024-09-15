package com.example.testspark;

import com.example.testspark.util.ShowDebugInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataConsumptionExample {

    private static final String JSON1_PATH = "./data/example_json_data.json";
    private static final String OPEN_DATA_OKIN = "./data/open_data/data-OKIN-2014.csv";
    private static final String REPORT_ON_CONTROL_ACTIVITIES = "./data/open_data/data-20180725-structure-20140926.xml";
    private static final String EXAMPLE_AVRO_FILE = "./data/userdata5.avro";
    private static final String EXAMPLE_ORC_FILE = "./data/bad_bloom_filter_1.6.0.orc";
    private static final String EXAMPLE_PARQUET_FILE = "./data/userdata1.parquet";

    private final JavaSparkContext sc;

    public DataConsumptionExample(JavaSparkContext sc) {
        this.sc = sc;
    }

    /**
     * Загрузка данных из файла parquet
     */
    public void loadParquetFile() {
        SparkSession spark = SparkSession.builder()
                .appName("Parquet")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("parquet")
                .load(EXAMPLE_PARQUET_FILE);

        ShowDebugInfo.getPartitionAndSchemaInfo(df, 10);
    }

    /**
     * Загрузка данных из файла orc
     */
    public void loadOrcFile() {
        SparkSession spark = SparkSession.builder()
                .appName("ORC")
                .config("spark.sql.orc.impl", "native")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("orc")
                .load(EXAMPLE_ORC_FILE);

        ShowDebugInfo.getPartitionAndSchemaInfo(df, 10);
    }

    /**
     * Загрузка данных из файла avro
     */
    public void loadAvroFile() {
        SparkSession spark = SparkSession.builder()
                .appName("Avro")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("avro")
                .load(EXAMPLE_AVRO_FILE);

        ShowDebugInfo.getPartitionAndSchemaInfo(df, 10);
    }

    /**
     * Загрузка данных из файла avro
     * @return возвращает данные полученные из файла
     */
    public Dataset<Row> getAvroFileData() {
        SparkSession spark = SparkSession.builder()
                .appName("Avro")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("avro")
                .load(EXAMPLE_AVRO_FILE);

        ShowDebugInfo.getPartitionAndSchemaInfo(df, 10);
        return df;
    }

    /**
     * Потребление данных из xml файла
     */
    public void createDatasetXml() {
        SparkSession spark = SparkSession.builder()
                .appName("Frame from xml")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .format("com.databricks.spark.xml")
                .option("rootTag", "banks")
                .option("rowTag", "bank")
                .load(REPORT_ON_CONTROL_ACTIVITIES);

        ShowDebugInfo.getPartitionAndSchemaInfo(df, 10);
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
     * Создание схемы для загружаемых данных csv
     */
    public void createStructure() {
        SparkSession spark = SparkSession.builder()
                .appName("Add schema ta data")
                .master("local")
                .getOrCreate();
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("Идентификатор записи", DataTypes.LongType , true),
                DataTypes.createStructField("Фасет", DataTypes.StringType , true),
                DataTypes.createStructField("Код позиции", DataTypes.StringType , true),
                DataTypes.createStructField("Уникальный код (составной ключ)", DataTypes.StringType , true),
                DataTypes.createStructField("Наименование", DataTypes.StringType , true),
                DataTypes.createStructField("Описание", DataTypes.StringType , true),
                DataTypes.createStructField("Дата актуализации", DataTypes.DateType , true),
                DataTypes.createStructField("Дата деактуализации", DataTypes.DateType , true),
                DataTypes.createStructField("Номер изменения (актуализации)", DataTypes.IntegerType , true),
                DataTypes.createStructField("Номер изменения (деактуализации)", DataTypes.IntegerType , true),
                DataTypes.createStructField("Дата введения в действие", DataTypes.DateType , true),
                DataTypes.createStructField("Статус записи", DataTypes.StringType , true)
        });
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ",")
                .option("dateFormat", "yyyy-MM-dd")
                .option("quote", "\"")
                .schema(schema)
                .load(OPEN_DATA_OKIN);

        ShowDebugInfo.getPartitionAndSchemaInfo(df);
    }

    public static String getExampleParquetPatch() {
        return EXAMPLE_PARQUET_FILE;
    }
}
