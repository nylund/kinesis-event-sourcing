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

class KinesisReader<T> {

    private Function<String, T> jsonMapFunc;
    private Consumer<List<T>> opFunc;

    KinesisClientLibConfiguration kinesisClientLibConfiguration;

    private Worker worker;

    KinesisReader(KinesisClientLibConfiguration kinesisClientLibConfiguration, Class<T> type, Consumer<List<T>> opFunc, StdDeserializer<T> jsonDeserialiser) {
        jsonMapFunc = new JsonMapper<>(type, jsonDeserialiser).get();
        this.opFunc = opFunc;
        this.kinesisClientLibConfiguration = kinesisClientLibConfiguration;
    }

    void init() {
        IRecordProcessorFactory recordProcessorFactory = getProcessorFactory();
        worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration, new NullMetricsFactory());

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
    }

    void shutdown() {
        try {
            worker.startGracefulShutdown().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
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
