package com.example.eventsourcing.data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "uuid_test")
public class InputModel {

    @Id
    byte[] id;

    Instant ts;

}
