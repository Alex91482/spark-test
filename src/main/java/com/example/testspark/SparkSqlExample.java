package com.example.testspark;

import com.example.testspark.config.SqlDbConfig;
import com.example.testspark.dao.impl.ExampleDAOImpl;
import com.example.testspark.dao.interfaces.ExampleDAO;
import com.example.testspark.util.Md5HashingUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.callUDF;

public class SparkSqlExample {

    private static final String CSV1_PATH = "./data/example_data.csv";
    private static final String COLUMN_NAME_MD5 = "_md5";

    private final SqlDbConfig sqlDbConfig;
    private final ExampleDAO exampleDAO;
    private final JavaSparkContext sc;

    public SparkSqlExample(JavaSparkContext sc) {
        this.sqlDbConfig = SqlDbConfig.getSqlDbConfig();
        this.exampleDAO = new ExampleDAOImpl();
        this.sc = sc;

        createCustomUdf(sc);
    }

    /**
     * Создание ользовательской функции
     * @param sc объект Spark-контекста
     */
    private void createCustomUdf(JavaSparkContext sc) {
        SQLContext sqlContext= new SQLContext(sc);
        sqlContext.udf().register(
                COLUMN_NAME_MD5,
                (UDF1<String, String>) Md5HashingUtil::getMd5Hash,
                DataTypes.StringType
        );
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
                        sqlDbConfig.getDdUri(),
                        exampleDAO.getTableAndSchema(),
                        sqlDbConfig.getProperties()
                );
    }
}
