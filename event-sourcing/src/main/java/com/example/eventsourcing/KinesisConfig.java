package com.example.eventsourcing;

import com.example.eventsourcingstarter.KinesisReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
public class KinesisConfig implements InitializingBean {

    private KinesisReader<InputRecord> reader;

    @Override
    public void afterPropertiesSet() throws Exception {
        reader = new KinesisReader<>(InputRecord.class, new JdbcUpdater().get());
        reader.init();
    }
}
