package com.example.eventsourcingstarter;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.util.function.Function;

/**
 * Map JSON in Kinesis payload to a POJO class
 */
public class JsonMapper<T> {

    private final Class<T> type;
    private static ObjectMapper objectMapper;
    private StdDeserializer<? extends T> deserializer;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public JsonMapper(Class<T> type) {
        this.type = type;
    }

    public JsonMapper(Class<T> type, StdDeserializer<T> deserializer) {
        this.type = type;
        this.deserializer = deserializer;

        SimpleModule module =
            new SimpleModule("CustomCarDeserializer",
            new Version(1, 0, 0, null, null, null));
        module.addDeserializer(type, deserializer);
        objectMapper.registerModule(module);
    }

    public Function<String, T> get() {
        return (json) -> {

            T t = null;
            try {
                t = objectMapper.readValue(json, type);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return t;
        };
    }

}
