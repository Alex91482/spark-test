package com.example.testspark.examples;

import com.example.testspark.foreachwriter.EmployeeChecker;
import com.example.testspark.generators.ThreadFileGenerator;
import com.example.testspark.util.FileHelper;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class SparkStreamingExample {

    private static final Logger logger = LoggerFactory.getLogger(SparkStreamingExample.class);

    private final JavaSparkContext sc;
    private final ThreadFileGenerator threadFileGenerator;

    public SparkStreamingExample(JavaSparkContext sc) {
        this.sc = sc;
        this.threadFileGenerator = new ThreadFileGenerator();
    }

    /**
     * Потоковое чтение файлов из двух директорий
     */
    public void  multipleReadFileFromDirectory() {
        var executeTime = 10;
        var directory1 = FileHelper.getTempStreamingDirectoryPath();
        var directory2 = FileHelper.getTempStreamingDirectoryPath2();
        startMultiFileGeneration(executeTime, Arrays.asList(directory1, directory2));

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("firstName", DataTypes.StringType , true),
                DataTypes.createStructField("lastName", DataTypes.StringType , true),
                DataTypes.createStructField("birthDate", DataTypes.DateType , true),
                DataTypes.createStructField("phoneNumber", DataTypes.LongType , true),
                DataTypes.createStructField("uuid", DataTypes.StringType , true),
                DataTypes.createStructField("jobTitle", DataTypes.StringType , true)
        });

        SparkSession spark = SparkSession.builder()
                .appName("Stream example")
                .master("local")
                .getOrCreate();
        Dataset<Row> df1 = spark
                .readStream()
                .format("json")
                .schema(schema)
                .option("multiLine", true)
                .load(directory1);
        Dataset<Row> df2 = spark
                .readStream()
                .format("json")
                .schema(schema)
                .option("multiLine", true)
                .load(directory2);
        try {
            StreamingQuery query1 = df1
                    .writeStream()
                    .outputMode(OutputMode.Append())
                    .foreach(new EmployeeChecker("query1"))
                    .start();
            StreamingQuery query2 = df2
                    .writeStream()
                    .outputMode(OutputMode.Append())
                    .foreach(new EmployeeChecker("query2"))
                    .start();

            var startProcessing = System.currentTimeMillis();
            while (query1.isActive() && query2.isActive()) {
                if ((startProcessing + executeTime * 1000) < System.currentTimeMillis()) {
                    query1.stop();
                    query2.stop();
                    logger.info("Stop streaming");
                }
                try {
                    Thread.sleep(2000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            logger.info("Query1 status: {}, query2 status: {}", query1.status(), query2.status());

        } catch (TimeoutException e) {
            e.printStackTrace();
        } finally {
            threadFileGenerator.shutdownExecutor();
        }
    }

    /**
     * Потоковая обработка файлов из директории.
     * Будет осуществляться поиск новых фвйлов в течении указанного времени executeTime.
     */
    public void readFileFromDirectory() {
        var directory = FileHelper.getTempStreamingDirectoryPath();
        var executeTime = 10;
        startFileGeneration(executeTime, Collections.singletonList(directory));

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("firstName", DataTypes.StringType , true),
                DataTypes.createStructField("lastName", DataTypes.StringType , true),
                DataTypes.createStructField("birthDate", DataTypes.DateType , true),
                DataTypes.createStructField("phoneNumber", DataTypes.LongType , true),
                DataTypes.createStructField("uuid", DataTypes.StringType , true),
                DataTypes.createStructField("jobTitle", DataTypes.StringType , true)
        });

        SparkSession spark = SparkSession.builder()
                .appName("Stream example")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark
                .readStream()
                .format("json")
                .schema(schema)
                .option("multiLine", true)
                .load(directory);
        try {
            StreamingQuery query = df
                    .writeStream()
                    .outputMode(OutputMode.Append())
                    .format("console")
                    .option("truncate", false)
                    .option("numRows", 3)
                    .start();
            query.awaitTermination(executeTime * 1000);

        } catch (TimeoutException | StreamingQueryException e) {
            e.printStackTrace();
        } finally {
            threadFileGenerator.shutdownExecutor();
        }
    }

    private void startFileGeneration(int executeTime, List<String> paths) {
        FileHelper.createOrCleanTempStreamingDirectory(paths);
        threadFileGenerator.createFilesInOneDirectories(executeTime);
    }

    private void startMultiFileGeneration(int executeTime, List<String> paths) {
        FileHelper.createOrCleanTempStreamingDirectory(paths);
        threadFileGenerator.createFilesInTwoDirectories(executeTime);
    }
}
