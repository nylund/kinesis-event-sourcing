package com.example.eventsourcing;

import com.example.eventsourcingstarter.InputRecord;
import com.example.eventsourcingstarter.InputRecordDeserializer;
import com.example.eventsourcingstarter.JdbcUpdater;
import com.example.eventsourcingstarter.KinesisReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.net.UnknownHostException;

@Component
public class KinesisConfig implements InitializingBean {

    private KinesisReader<InputRecord> reader;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void afterPropertiesSet() throws Exception {
        new Thread(() -> {
            reader = new KinesisReader<>(
                InputRecord.class,
                new JdbcUpdater(jdbcTemplate).get(),
                new InputRecordDeserializer());
            try {
                reader.init();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }).start();

        int x = 1;
        x++;
    }
}
