package com.example.testspark.service.data_analysis.reducers;

import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;

public class CharacterCountReducer implements ReduceFunction<Row>{

    private static final long serialVersionUID = 45679456L;

    @Override
    public Row call(Row row0, Row row1) throws Exception {
        var list = new ArrayList<Integer>();

        if (row0.size() != row1.size()) {
            throw new RuntimeException("Size row0: " + row0.size() + ", size row1: " + row1.size());
        }
        for (int i = 0; i < row1.size(); i++) {
            list.add(Math.max(row0.getInt(i), row1.getInt(i)));
        }
        return RowFactory.create(list.toArray());
    }
}
