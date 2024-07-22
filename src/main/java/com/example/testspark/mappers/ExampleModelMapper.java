package com.example.testspark.mappers;

import com.example.testspark.dao.entity.ExampleModel;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.io.Serial;

public class ExampleModelMapper implements MapFunction<Row, ExampleModel> {

    @Serial
    private static final long serialVersionUID = 42L;

    public ExampleModel call(Row value) {
        var example = new ExampleModel();
        example.setGuid(value.getAs("guid"));
        example.setTitle(value.getAs("title"));
        example.setIndex1(Integer.parseInt(value.getAs("index_1")));
        example.setDateAdded(Long.parseLong(value.getAs("dateAdded")));
        example.setLastModified(Long.parseLong(value.getAs("lastModified")));
        example.setId(Integer.parseInt(value.getAs("id")));
        example.setTypeCode(Integer.parseInt(value.getAs("typeCode")));
        example.setIconuri(value.getAs("iconuri"));
        example.setType1(value.getAs("type_1"));
        example.setUri(value.getAs("uri"));
        example.setMd5(value.getAs("md5"));
        return example;
    }
}
