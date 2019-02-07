package com.example.eventsourcing;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.example.eventsourcingstarter.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@Slf4j
public class KinesisConfig implements InitializingBean, DisposableBean {

    private String kinesisStreamName = "test-input-stream-1";
    private String kinesisApplicationName = "SampleKinesisApplication";
    private String kinesisEndpoint = "http://localhost:4568";
    private String dynamoDbEndpoint = "http://localhost:4569";
    private String awsRegion = "us-east-1";

    private InputRecordConsumer kinesisConsumer;
    private ExecutorService executorService;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Override
    public void afterPropertiesSet() throws Exception {

        AWSCredentialsProvider credentialsProvider = new EnvironmentVariableCredentialsProvider();
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();

        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(kinesisApplicationName,
                        kinesisStreamName,
                        credentialsProvider,
                        workerId)
                        .withKinesisEndpoint(kinesisEndpoint)
                        .withDynamoDBEndpoint(dynamoDbEndpoint)
                        .withRegionName(awsRegion)
                        .withInitialPositionInStream(InitialPositionInStream.LATEST);

        executorService = Executors.newSingleThreadExecutor();

        Callable<Void> runConsumer = () -> {
            kinesisConsumer = new InputRecordConsumer(kinesisClientLibConfiguration, new JdbcUpdater(jdbcTemplate).get());
            kinesisConsumer.start();

            return null;
        };

        executorService.submit(runConsumer);
    }

    @Override
    public void destroy() throws Exception {
        log.debug("Stopping all threads");

        kinesisConsumer.stop();
        executorService.shutdown();
    }
}
