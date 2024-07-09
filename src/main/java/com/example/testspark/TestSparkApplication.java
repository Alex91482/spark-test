package com.example.testspark;

import com.example.testspark.config.Init;
import com.example.testspark.util.Md5HashingUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;


public class TestSparkApplication {

    private static final String COLUMN_NAME_MD5 = "_md5";

    public static void main(String[]args) {

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount")
                .setMaster("local")
                .set("spark.executor.memory","1g");

        try (var sc = new JavaSparkContext(sparkConf)) {

            init(sc);

            var datasetExample = new SparkDatasetExample(sc);
            datasetExample.conversionOperations();
            //datasetExample.createDatasetExampleModel();
            //datasetExample.createDatasetString();
            //datasetExample.joinDataCsvJson();
            //datasetExample.loadJsonFile();
            //datasetExample.loadCsvFile();
            var rddExample = new SparkRddExample(sc);
            //rddExample.readReadmeFile();
            var sqlExample = new SparkSqlExample(sc);
            //sqlExample.readCsvAndSaveToDb();
        }
    }

    private static void init(JavaSparkContext sc) {
        //Init.execute(); //создание схемы и таблиц
        createCustomUdf(sc); //инициализация кастомных функций
    }

    /**
     * Создание ользовательской функции
     * @param sc версия SparkContext, совместимая с Java, которая возвращает JavaRDD и работает с коллекциями Java
     */
    private static void createCustomUdf(JavaSparkContext sc) {
        SQLContext sqlContext= new SQLContext(sc);
        sqlContext.udf().register(
                COLUMN_NAME_MD5,
                (UDF1<String, String>) Md5HashingUtil::getMd5Hash,
                DataTypes.StringType
        );
    }
}
