package com.example.eventsourcingstarter;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
class KinesisReader<T> {

    private Function<String, T> jsonMapFunc;
    private Consumer<List<T>> opFunc;
    private KinesisClientLibConfiguration kinesisClientLibConfiguration;
    private Worker worker;

    public KinesisReaderBuilder<T> builder() {
        return new KinesisReaderBuilder<T>();
    }

    public static class KinesisReaderBuilder<T> {
        private KinesisClientLibConfiguration kinesisClientLibConfiguration;
        private Class<T> type;
        private Consumer<List<T>> opFunc;
        private StdDeserializer<T> jsonDeserialiser;

        public KinesisReaderBuilder<T> setKinesisClientLibConfiguration(KinesisClientLibConfiguration kinesisClientLibConfiguration) {
            this.kinesisClientLibConfiguration = kinesisClientLibConfiguration;
            return this;
        }

        public KinesisReaderBuilder<T> setType(Class<T> type) {
            this.type = type;
            return this;
        }

        public KinesisReaderBuilder<T> setOpFunc(Consumer<List<T>> opFunc) {
            this.opFunc = opFunc;
            return this;
        }

        public KinesisReaderBuilder<T> setJsonDeserialiser(StdDeserializer<T> jsonDeserialiser) {
            this.jsonDeserialiser = jsonDeserialiser;
            return this;
        }

        public KinesisReader<T> build() {
            return new KinesisReader<T>(kinesisClientLibConfiguration, type, opFunc, jsonDeserialiser);
        }
    }

    private KinesisReader(KinesisClientLibConfiguration kinesisClientLibConfiguration, Class<T> type, Consumer<List<T>> opFunc,
                  StdDeserializer<T> jsonDeserialiser) {
        jsonMapFunc = new JsonMapper<>(type, jsonDeserialiser).get();
        this.opFunc = opFunc;
        this.kinesisClientLibConfiguration = kinesisClientLibConfiguration;
    }

    void init() {
        IRecordProcessorFactory recordProcessorFactory = getProcessorFactory();
        worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration, new NullMetricsFactory());

        try {
            worker.run();
        } catch (Throwable t) {
            log.error("Kinesis worker failed", t);
        }
    }

    void shutdown() {
        try {
            worker.startGracefulShutdown().get();
        } catch (InterruptedException e) {
            log.error("Kinesis worker interrupted", e);
        } catch (ExecutionException e) {
            log.error("Kinesis worker failed", e);
        }
    }

    private IRecordProcessorFactory getProcessorFactory() {
        return this::getProcessor;
    }

    private IRecordProcessor getProcessor() {

        final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

        return new IRecordProcessor() {

            @Override
            public void initialize(String s) {

            }

            @Override
            public void processRecords(List<Record> list, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

                List<T> processed = list.stream()
                    .map(record -> {
                        try {
                            return decoder.decode(record.getData()).toString();
                        } catch (CharacterCodingException e) {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .map(jsonMapFunc)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toCollection(ArrayList::new));

                opFunc.accept(processed);
            }

            @Override
            public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

            }
        };
    }
}
