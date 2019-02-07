package com.example.eventsourcingstarter;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

import java.net.UnknownHostException;
import java.util.List;
import java.util.function.Consumer;

public class InputRecordConsumer {

    KinesisReader<InputRecord> consumer;

    public InputRecordConsumer(KinesisClientLibConfiguration kinesisClientLibConfiguration, Consumer<List<InputRecord>> opFunc) {
        consumer = new KinesisReader<>(kinesisClientLibConfiguration, InputRecord.class, opFunc, new InputRecordDeserializer());
    }

    public void start() throws UnknownHostException {
        consumer.init();
    }

    public void stop() {
        consumer.shutdown();
    }

}
