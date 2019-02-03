package com.example.eventsourcingstarter;

import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.common.ConfigsBuilder;

import java.net.URI;
import java.util.UUID;

import software.amazon.awssdk.regions.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
/*
KCL2 not supported by localstack

https://github.com/mhart/kinesalite/issues/75

 */
public class KinesisReaderKCL2 {

    String streamName = "test-input-stream-1";
    String applicationName = "test";

    public void init() {
        Region region = Region.US_EAST_1;
        KinesisAsyncClient kinesisClient = KinesisAsyncClient.builder()
                .region(region)
                .endpointOverride(URI.create("http://localhost:4568"))
                .httpClient(NettyNioAsyncHttpClient.builder().protocol(Protocol.HTTP1_1).build())
                .build();
        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder()
                .region(region)
                .endpointOverride(URI.create("http://localhost:4569"))
                .httpClient(NettyNioAsyncHttpClient.builder().protocol(Protocol.HTTP1_1).build())
                .build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();

        ConfigsBuilder configsBuilder = new ConfigsBuilder(streamName, applicationName, kinesisClient, dynamoClient,
                cloudWatchClient, UUID.randomUUID().toString(), getProcessorFactory());

        // https://github.com/awslabs/amazon-kinesis-client/issues/429
        PollingConfig pollingConfig = new PollingConfig(streamName, kinesisClient);
        RetrievalConfig retrievalConfig = new RetrievalConfig(kinesisClient, streamName, applicationName).retrievalSpecificConfig(pollingConfig);

        Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig
        );

        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();
    }

    private ShardRecordProcessorFactory getProcessorFactory() {
        return () -> getRecordProcessor();
    }

    private ShardRecordProcessor getRecordProcessor() {
        return new ShardRecordProcessor() {
            @Override
            public void initialize(InitializationInput initializationInput) {

                int x = 1;
                x++;
            }

            @Override
            public void processRecords(ProcessRecordsInput processRecordsInput) {
                processRecordsInput.records().stream().forEach(record -> {
                    record = null;
                });

            }

            @Override
            public void leaseLost(LeaseLostInput leaseLostInput) {

            }

            @Override
            public void shardEnded(ShardEndedInput shardEndedInput) {

            }

            @Override
            public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {

            }
        };
    }
}
