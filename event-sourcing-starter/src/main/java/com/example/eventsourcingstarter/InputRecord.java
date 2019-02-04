package com.example.eventsourcingstarter;

import lombok.Data;

import java.time.Instant;
import java.util.UUID;

@Data
public class InputRecord {

    UUID id;

    Instant ts;

}
