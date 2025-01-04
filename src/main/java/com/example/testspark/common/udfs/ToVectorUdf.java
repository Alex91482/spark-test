package com.example.testspark.common.udfs;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;

import org.apache.spark.sql.api.java.UDF5;

public class ToVectorUdf implements UDF5<Double, Double, Double, Double, Double, Vector> {

    private static final long serialVersionUID = -216751L;

    @Override
    public Vector call(Double dayOfTheWeek, Double day, Double month, Double intervalStart, Double intervalEnd) {
        return Vectors.dense(dayOfTheWeek, day, month, intervalStart, intervalEnd);
    }
}
