package com.example.testspark.util.serializes;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class LocalDateTimeSerializer extends JsonSerializer<LocalDateTime> {

    @Override
    public void serialize(LocalDateTime value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        try {
            String s = value.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
            gen.writeString(s);
        } catch (DateTimeParseException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            gen.writeString("");
        }
    }
}
