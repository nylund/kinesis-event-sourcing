package com.example.eventsourcing;

import com.example.eventsourcingstarter.JsonMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class InputRecordSerializerTests {

    @Test
    public void deserialiseInputRecord() {
        JsonMapper<InputRecord> mapper = new JsonMapper<>(InputRecord.class, new InputRecordDeserializer());

        String input = "{\n" +
                "  \"id\": \"4afe2f8b-454d-4146-8214-1472b9add3a7\", " +
                "  \"ts\": \"2015-10-27T16:22:27.605-07:00\" " +
                "}";

        InputRecord record = mapper.get().apply(input);

        assertNotNull(record);
        assertNotNull(record.getId());
        assertEquals("4afe2f8b-454d-4146-8214-1472b9add3a7", record.getId().toString());

        assertNotNull(record.getTs());
    }
}
