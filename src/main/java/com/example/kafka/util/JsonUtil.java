package com.example.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class JsonUtil {


    public static final ObjectMapper strict = new ObjectMapper();


    static {
        strict.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        strict.registerModule(new JavaTimeModule());
    }

    public static String writeValueAsString(Object value) {

        if (value == null)
            return null;

        try {
            return strict.registerModule(new JavaTimeModule()).writeValueAsString(value);

        } catch (JsonProcessingException e) {
            log.error(e.getLocalizedMessage(), e);
            return null;
        }
    }

    public static String writeValueAsString2(Object value) {

        if (value == null)
            return null;

        try {
            return strict.writeValueAsString(value);

        } catch (JsonProcessingException e) {
            log.error(e.getLocalizedMessage(), e);

            return null;
        }
    }


    public static <T> T readValue(String content, Class<T> valueType) throws IOException {

        if (content == null)
            return null;

        return strict.readValue(content, valueType);
    }


    public static <T> T readValue(String content, TypeReference<T> valueType) throws IOException {

        if (content == null)
            return null;

        return strict.readValue(content, valueType);
    }


}
