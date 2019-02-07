package com.example.eventsourcingstarter;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

import java.util.List;
import java.util.function.Consumer;

public class InputRecordConsumer {

    KinesisReader<InputRecord> consumer;

    public InputRecordConsumer(KinesisClientLibConfiguration kinesisClientLibConfiguration,
                               Consumer<List<InputRecord>> opFunc) {

        consumer = new KinesisReader.KinesisReaderBuilder<InputRecord>()
            .setKinesisClientLibConfiguration(kinesisClientLibConfiguration)
            .setType(InputRecord.class)
            .setJsonDeserialiser(new InputRecordDeserializer())
            .setOpFunc(opFunc)
            .build();
    }

    public void start() {
        consumer.init();
    }

    public void stop() {
        consumer.shutdown();
    }

}
