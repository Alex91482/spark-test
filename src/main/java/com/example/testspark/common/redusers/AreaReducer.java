package com.example.testspark.common.redusers;

import org.apache.spark.api.java.function.ReduceFunction;

public class AreaReducer implements ReduceFunction<Double> {

    private static final long serialVersionUID = 45679L;

    @Override
    public Double call(Double t0, Double t1) throws Exception {
        return t0 + t1;
    }
}
