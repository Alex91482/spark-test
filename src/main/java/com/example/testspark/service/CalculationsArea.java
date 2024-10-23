package com.example.testspark.service;

import com.example.testspark.mappers.AreaMapper;
import com.example.testspark.redusers.AreaReducer;
import com.example.testspark.service.data_analysis.ShowDebugInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;

public class CalculationsArea {

    /**
     * Создание 100 неправильных четырехугольников с последующем вычмслением площади каждого и сложением всех площадей
     */
    public void execute() {
        SparkSession spark = SparkSession.builder()
                .appName("Calculations Area")
                .master("local[*]")
                .getOrCreate();
        var coordinatesOfFigures = new ArrayList<Tuple4<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>();
        for (int i = 0; i < 100; i++) {
            var fourPoint = new ArrayList<Tuple2<Integer, Integer>>();
            for (int i1 = 0; i1 < 4; i1++) {
                int x = (int) (1 + Math.random() * 10);
                int y = (int) (1 + Math.random() * 10);
                fourPoint.add(new Tuple2<>(x, y));
            }
            coordinatesOfFigures.add(new Tuple4<>(fourPoint.get(0), fourPoint.get(1), fourPoint.get(2), fourPoint.get(3)));
        }
        Dataset<Row> figuresData = spark
                .createDataset(coordinatesOfFigures, Encoders.tuple(
                        Encoders.tuple(Encoders.INT(), Encoders.INT()),
                        Encoders.tuple(Encoders.INT(), Encoders.INT()),
                        Encoders.tuple(Encoders.INT(), Encoders.INT()),
                        Encoders.tuple(Encoders.INT(), Encoders.INT())
                        )
                )
                .toDF();
        ShowDebugInfo.getPartitionAndSchemaInfo(figuresData, 5);
        Dataset<Double> area = figuresData.map(new AreaMapper(), Encoders.DOUBLE());
        ShowDebugInfo.getPartitionAndSchemaInfo(area, 5);
        double sumArea = area.reduce(new AreaReducer());
        System.out.println("Sum of areas of all quadrilaterals: " + sumArea);
    }
}
