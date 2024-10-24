package com.example.testspark.service.data_analysis.mapers;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;

public class CharacterCountMapper implements MapFunction<Row, Row> {

    private static final long serialVersionUID = 4567932L;

    @Override
    public Row call(Row row) throws Exception {
        var list = new ArrayList<Integer>();
        for (int i = 0; i < row.size(); i++) {
            if (row.getAs(i) == null) {
                list.add(0);
                continue;
            }
            switch (row.getAs(i).getClass().getSimpleName()) {
                case "Integer":
                    list.add(String.valueOf((int) row.getAs(i)).length());
                    break;
                case "Long":
                    list.add(String.valueOf((long) row.getAs(i)).length());
                    break;
                case "Character":
                    list.add(String.valueOf((char) row.getAs(i)).length());
                    break;
                case "String":
                    list.add(String.valueOf((String) row.getAs(i)).length());
                    break;
                case "Double":
                    list.add(String.valueOf((double) row.getAs(i)).length());
                    break;
                case "Float":
                    list.add(String.valueOf((float) row.getAs(i)).length());
                    break;
                case "Boolean":
                    list.add(String.valueOf((boolean) row.getAs(i)).length());
                    break;
                case "Byte":
                    list.add(String.valueOf((byte) row.getAs(i)).length());
                    break;
                default:
                    list.add(-1);
            }
        }
        return RowFactory.create(list.toArray());
    }
}
