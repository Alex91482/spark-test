package com.example.testspark.redusers;

import org.apache.spark.api.java.function.ReduceFunction;

import java.io.Serial;

public class AreaReducer implements ReduceFunction<Double> {

    @Serial
    private static final long serialVersionUID = 45679L;

    @Override
    public Double call(Double t0, Double t1) throws Exception {
        return t0 + t1;
    }
}
