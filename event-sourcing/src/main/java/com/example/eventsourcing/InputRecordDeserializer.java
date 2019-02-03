package com.example.eventsourcing;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class InputRecordDeserializer extends StdDeserializer<InputRecord> {

    final static DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_DATE_TIME;

    public InputRecordDeserializer() {
        this(null);
    }

    public InputRecordDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public InputRecord deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException, JsonProcessingException {

        InputRecord res = new InputRecord();

        ObjectCodec codec = jsonParser.getCodec();
        JsonNode node = codec.readTree(jsonParser);

        if (!node.has("id")) {
            throw JsonMappingException.from(jsonParser, "Required field missing");
        }
        res.setId(UUID.fromString(node.get("id").asText()));

        if (!node.has("ts")) {
            throw JsonMappingException.from(jsonParser, "Required field missing");
        }
        res.setTs(Instant.from(OffsetDateTime.parse(node.get("ts").asText(), timeFormatter)));

        return res;
    }

}
