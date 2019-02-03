package com.example.eventsourcingstarter;

import lombok.Data;
import org.junit.Test;
import static org.junit.Assert.*;

public class JsonMapperTest {

    @Data
    public static class Pojo {
        String foo;
    }

    private JsonMapper<Pojo> mapper = new JsonMapper<>(Pojo.class);

    @Test
    public void mapToPojo() {

        String inputJson = "{\"foo\":\"bar\"}";
        Pojo result = mapper.get().apply(inputJson);

        assertNotNull(result);
        assertEquals("bar", result.getFoo());
    }

    @Test
    public void handleUnknownParametersInInput() {
        String inputJson = "{\"foo\":\"bar\", \"notInPojo\":false}";
        Pojo result = mapper.get().apply(inputJson);

        assertNotNull(result);
        assertEquals("bar", result.getFoo());
    }

}
