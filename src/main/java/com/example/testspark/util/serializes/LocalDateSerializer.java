package com.example.testspark.util.serializes;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;


import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class LocalDateSerializer extends JsonSerializer<LocalDate> {

    @Override
    public void serialize(LocalDate value, JsonGenerator gen, SerializerProvider provider)
            throws IOException {
        try {
            String s = value.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            gen.writeString(s);
        } catch (DateTimeParseException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            gen.writeString("");
        }
    }
}
