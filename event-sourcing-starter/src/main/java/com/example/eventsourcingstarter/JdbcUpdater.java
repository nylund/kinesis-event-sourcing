package com.example.eventsourcingstarter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class JdbcUpdater {

    JdbcTemplate jdbcTemplate;

    public JdbcUpdater(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Consumer<List<InputRecord>> get() {
        final String upsertQuery = "INSERT INTO input_records " +
                "   (id, ts)" +
                " values (" +
                "   UNHEX(REPLACE(?, '-', ''))" +
                "  ,now()" +
                " ) " +
                " ON DUPLICATE KEY UPDATE" +
                "   ts = now()" +
                ";";

        return (records) -> {

            jdbcTemplate.batchUpdate(upsertQuery, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    ps.setString(1, records.get(i).getId().toString());
                }

                @Override
                public int getBatchSize() {
                    return 1;
                }
            });
        };
    }


}