package com.example.eventsourcingstarter;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by corrupt on 02/02/2019.
 */
public class KinesisReader<T> {

    public static final String SAMPLE_APPLICATION_STREAM_NAME = "test-input-stream-1";
    private static final String SAMPLE_APPLICATION_NAME = "SampleKinesisApplication";

    private Function<String, T> jsonMapFunc;
    private Consumer<List<T>> opFunc;

    public KinesisReader(Class<T> type, Consumer<List<T>> opFunc, StdDeserializer<T> jsonDeserialiser) {
        jsonMapFunc = new JsonMapper<>(type, jsonDeserialiser).get();
        this.opFunc = opFunc;
    }

    public void init() throws UnknownHostException {
        AWSCredentialsProvider credentialsProvider = new EnvironmentVariableCredentialsProvider();
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();

        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(SAMPLE_APPLICATION_NAME,
                        SAMPLE_APPLICATION_STREAM_NAME,
                        credentialsProvider,
                        workerId)
                .withKinesisEndpoint("http://localhost:4568")
                .withDynamoDBEndpoint("http://localhost:4569")
                .withRegionName("us-east-1");


        kinesisClientLibConfiguration.withInitialPositionInStream(InitialPositionInStream.LATEST);

        IRecordProcessorFactory recordProcessorFactory = getProcessorFactory();
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration, new NullMetricsFactory());

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
    }

    private IRecordProcessorFactory getProcessorFactory() {
        return () -> getProcessor();
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
