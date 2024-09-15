package com.example.testspark;

import com.example.testspark.config.PostgresSqlDbConfig;
import com.example.testspark.dao.impl.ExampleDAOImpl;
import com.example.testspark.dao.interfaces.ExampleDAO;
import com.example.testspark.util.ShowDebugInfo;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.callUDF;

public class SparkSqlExample {

    private static final String CSV1_PATH = "./data/example_data.csv";
    private static final String COLUMN_NAME_MD5 = "_md5";

    private final PostgresSqlDbConfig postgresSqlDbConfig;
    private final ExampleDAO exampleDAO;
    private final JavaSparkContext sc;

    public SparkSqlExample(JavaSparkContext sc) {
        this.postgresSqlDbConfig = PostgresSqlDbConfig.getSqlDbConfig();
        this.exampleDAO = new ExampleDAOImpl();
        this.sc = sc;
    }

    /**
     * Создание представления view для использования sql запроса
     */
    public void createTempView() {
        SparkSession spark = SparkSession.builder()
                .appName("Sql view")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark
                .read()
                .format("parquet")
                .load(DataConsumptionExample.getExampleParquetPatch());
        df.createOrReplaceTempView("parquet_data");

        ShowDebugInfo.getPartitionAndSchemaInfo(df, 5);

        Dataset<Row> selection = spark.sql("SELECT * FROM parquet_data WHERE salary > 150000 and country = 'China' ORDER BY last_name");

        ShowDebugInfo.getPartitionAndSchemaInfo(selection, 10);
    }

    /**
     * Метод для выполения простых запросов на выборку из бд
     * @param spark ссылка на SparkSession
     * @param sql запрос sql
     * @return набор данных
     */
    private Dataset<Row> executeQuerySelect(SparkSession spark, String sql) {
        return spark.read().jdbc(
                postgresSqlDbConfig.getDdUri(),
                "(" + sql + ") example_0",
                postgresSqlDbConfig.getProperties()
        );
    }

    /**
     * Получить данный из БД Postgres, из таблмцы example_table находящийся в схеме example
     */
    public void getDataExampleTable() {
        SparkSession spark = SparkSession.builder()
                .appName("Postgres get data")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read().jdbc(
                postgresSqlDbConfig.getDdUri(),
                exampleDAO.getTableAndSchema(),
                postgresSqlDbConfig.getProperties()
        );
        df = df.orderBy(df.col("md5"));
        var sql = "select * " +
                "from example.example_table ee " +
                "where ee.index_1 like '%2%' and ee.iconuri like '%www%'";
        Dataset<Row> df1 = executeQuerySelect(spark, sql);

        ShowDebugInfo.getPartitionAndSchemaInfo(df, 10);
        ShowDebugInfo.getPartitionAndSchemaInfo(df1, 10);
    }

    /**
     * Метод сохраняет прочитаный csv в БД
     * Добавляется колонка md5 в которую записывается хеш md5 от колонки guid
     */
    public void readCsvAndSaveToDb() {
        SparkSession spark = SparkSession.builder()
                .appName("CSC to DB")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load(CSV1_PATH);

        df = df.withColumn("md5", callUDF(COLUMN_NAME_MD5, df.col("guid")));
        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(
                        postgresSqlDbConfig.getDdUri(),
                        exampleDAO.getTableAndSchema(),
                        postgresSqlDbConfig.getProperties()
                );
    }
}
