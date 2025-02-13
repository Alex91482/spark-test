package com.example.testspark.examples;

import com.example.testspark.service.data_analysis.ShowDebugInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkExifExample {

    private static final String PHOTO_DATA_PATCH = "./data/photo";

    public SparkExifExample() {}

    /**
     * Получение мета данных из фотографий
     */
    public void getPhotoExifData() {
        SparkSession spark = SparkSession.builder()
                .appName("exif data")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("com.jpg.spark.exif.ExifDirectoryDataSourceShortnameAdvertiser")
                .option("recursive", "true")
                .option("limit", "100000")
                .option("extensions", "jpg")
                .load(PHOTO_DATA_PATCH);

        ShowDebugInfo.getPartitionAndSchemaInfo(df, 5);
    }
}
