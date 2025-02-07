package com.example.testspark;

import com.example.testspark.examples.DataConsumptionExample;
import com.example.testspark.examples.SparkClickHouseExample;
import com.example.testspark.examples.SparkDatasetExample;
import com.example.testspark.examples.SparkElasticSearchExample;
import com.example.testspark.examples.SparkExifExample;
import com.example.testspark.examples.SparkRddExample;
import com.example.testspark.examples.SparkSqlExample;
import com.example.testspark.examples.SparkStreamingExample;
import com.example.testspark.service.CalculationsArea;
import com.example.testspark.util.FileHelper;
import com.example.testspark.util.Md5HashingUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;


public class TestSparkApplication {

    private static final String COLUMN_NAME_MD5 = "_md5";
    private static final String COLUMN_NAME_CHAR_COUNTER = "_countChar";

    public static void main(String[]args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaWordCount")
                .setMaster("local") //local[1] в скобках указывается количество системных потоков
                .set("spark.executor.memory","10g");

        try (var sc = new JavaSparkContext(sparkConf)) {

            var sparkStreamingExample = new SparkStreamingExample(sc);
            var sparkExifExample = new SparkExifExample();
            var clickHouseExample = new SparkClickHouseExample(sc);
            var elasticSearchExample = new SparkElasticSearchExample(sc);
            var dataConsumption = new DataConsumptionExample(sc);
            var calculationArea = new CalculationsArea();
            var datasetExample = new SparkDatasetExample(sc);
            var rddExample = new SparkRddExample(sc);
            var sqlExample = new SparkSqlExample(sc);

            init(sc, dataConsumption, elasticSearchExample);

            // streaming
            //sparkStreamingExample.readFileFromDirectory();
            //sparkStreamingExample.multipleReadFileFromDirectory();

            // exif
            //sparkExifExample.getPhotoExifData();

            // clickhouse
            //clickHouseExample.getData();

            // elasticsearchExample
            //elasticSearchExample.getData();

            // dataConsumption
            //dataConsumption.loadParquetFile();
            //dataConsumption.loadOrcFile();
            //dataConsumption.loadAvroFile();
            //dataConsumption.createStructure();
            //dataConsumption.createDatasetXml();
            //dataConsumption.loadJsonFile();

            // calculationArea
            //calculationArea.execute();

            // datasetExample
            //datasetExample.conversionOperations();
            //datasetExample.createDatasetExampleModel();
            //datasetExample.createDatasetString();
            //datasetExample.joinDataCsvJson();
            //datasetExample.loadJsonFile();
            //datasetExample.loadCsvFile();
            datasetExample.filterData();

            // rddExample
            //rddExample.readReadmeFile();

            // sqlExample
            //sqlExample.createGlobalTempView();
            //sqlExample.createTempView();
            //sqlExample.readCsvAndSaveToDb();
            //sqlExample.getDataExampleTable();
        }
    }

    private static void init(JavaSparkContext sc, DataConsumptionExample dataConsumption, SparkElasticSearchExample elasticSearchExample) {
        //Init.execute(); //создание схемы и таблиц
        //Init.executeElastic(); //создание раздела
        //Init.initDataElasticSearch(dataConsumption, elasticSearchExample); //заполнение данными
        var directories = Arrays.asList(FileHelper.getTempStreamingDirectoryPath(), FileHelper.getTempStreamingDirectoryPath2());
        FileHelper.createOrCleanTempStreamingDirectory(directories); //создать или очистить директорию для временных файлов
        createCustomUdf(sc); //инициализация кастомных функций
    }

    /**
     * Создание пользовательской функции
     * @param sc версия SparkContext, совместимая с Java, которая возвращает JavaRDD и работает с коллекциями Java
     */
    private static void createCustomUdf(JavaSparkContext sc) {
        SQLContext sqlContext= new SQLContext(sc);
        //хеширование одной из колонок по алгоритму md5
        sqlContext.udf().register(
                COLUMN_NAME_MD5,
                (UDF1<String, String>) Md5HashingUtil::getMd5Hash,
                DataTypes.StringType
        );
        //подсчет количества символов в колонке
        sqlContext.udf().register(COLUMN_NAME_CHAR_COUNTER, (UDF1<String, Integer>) str -> {
            if (str != null) {
                return str.length();
            } else {
                return 0;
            }
        }, DataTypes.IntegerType);
    }
}
