package com.example.testspark;

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

import java.util.concurrent.TimeoutException;

public class SparkStreamingExample {

    private static final Logger logger = LoggerFactory.getLogger(SparkStreamingExample.class);

    private final JavaSparkContext sc;

    public SparkStreamingExample(JavaSparkContext sc) {
        this.sc = sc;
    }

    public void readFileFromDirectory() {
        int executeTime = 10;
        startFileGeneration(executeTime);
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
                .load(FileHelper.getTempStreamingDirectoryPath());
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
        }
    }

    private void startFileGeneration(int executeTime) {
        var threadFileGenerator = new ThreadFileGenerator();
        FileHelper.createOrCleanTempStreamingDirectory();
        threadFileGenerator.execute(executeTime);
    }
}
