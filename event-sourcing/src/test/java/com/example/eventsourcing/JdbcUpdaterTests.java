package com.example.eventsourcing;

import com.example.eventsourcingstarter.InputRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mvnsearch.h2.H2FunctionsLoader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class JdbcUpdaterTests {

    JdbcTemplate jdbcTemplate;

    @Before
    public void setup() {
        DataSource dataSource = new EmbeddedDatabaseBuilder()
            .setType(EmbeddedDatabaseType.H2)
            .setName("test-db;DATABASE_TO_UPPER=false;MODE=MYSQL")
            .addScript("db/V1__input_record_v1.sql")
            .build();

        H2FunctionsLoader.loadMysqlFunctions(dataSource);

        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @After
    public void dbCleanup() {
        jdbcTemplate.execute("drop all objects");
    }

    @Test
    public void handleListWithOneRecord() {
        List<InputRecord> input = new ArrayList<>();
        input.add(createRecord(UUID.randomUUID(), Instant.now()));

        new JdbcUpdater(jdbcTemplate).get().accept(input);
    }

    @Test
    public void handleListWithMultipleRecords() {
        List<InputRecord> input = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            input.add(createRecord(UUID.randomUUID(), Instant.now()));
        }

        new JdbcUpdater(jdbcTemplate).get().accept(input);
    }

    private InputRecord createRecord(UUID uuid, Instant ts) {
        InputRecord record = new InputRecord();
        record.setId(UUID.randomUUID());
        record.setTs(Instant.now());

        return record;
    }
}