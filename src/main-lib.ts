import 'reflect-metadata';
import { KafkaClient } from './kafka/kafka-client';
import { KafkaConsumer } from 'node-rdkafka';
import { KafkaOptionKeys } from './kafka/kafka-option-keys';
import { KafkaClientMessage } from './kafka/kafka-message';
import { KafkaLogEvent, KafkaLogSeverity, KafkaLogUtil } from './kafka/kafka-log';
import Container from 'typedi';
import { LoggingService } from './core/logging-service';

/*
List of confluent and Kafka commands to be used.
- confluent stop
- confluent start
- kafka-topics --list --zookeeper localhost:2181
- kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic test2
- kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group test-group
- kafka-console-consumer --bootstrap-server localhost:9092 --topic test2 --group test-group --from-beginning
- kafka-console-producer --broker-list localhost:9092 --topic test2
- kafka-topics --delete --zookeeper localhost:2181  --topic test2
- kafka-topics --list --zookeeper localhost:2181
- kafka-topics --zookeeper localhost:2181 --describe --topic test2

Auto refresh
- while true; do kafka-topics --zookeeper localhost:2181 --describe --topic test; sleep .5; done
- while true; do kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group test-group; sleep .5; done

- node ./lib/kafka/main-test.js
*/

const kafkaClientOptions = {
    [KafkaOptionKeys.METADATA_BROKER_LIST]: 'localhost:9092',
    // a uuid is auto created by kafka and appended to CLIENT-Id you specified to compose the CONSUMER-ID
    [KafkaOptionKeys.CLIENT_ID]: 'main',
    [KafkaOptionKeys.FETCH_WAIT_MAX_MS]: 700,
    [KafkaOptionKeys.FETCH_MIN_BYTES]: 1,
    [KafkaOptionKeys.GROUP_ID]: 'test-group',
    [KafkaOptionKeys.SESSION_TIMEOUT_MS]: 6000,
    [KafkaOptionKeys.ENABLE_AUTO_COMMIT]: true,
    [KafkaOptionKeys.AUTO_COMMIT_INTERVAL_MS]: 5000,
    [KafkaOptionKeys.HEARTBEAT_INTERVAL_MS]: 3000
};

// test2 should contains partitions
const topics = ['rqm-entity'];

// 3 concurrent handlers
const maxConcurrentMsgProcessing = 3;

// Handler processing timeout 10 sec
const handlerProcessingTimeoutSec = 10;

// to be able to use the kafka-console-producer
// (See kafka-console-producer --broker-list localhost:9092 --topic test2)
const providerType = 'kafka-provider';

const loggingService: LoggingService = Container.get(LoggingService);
const client = new KafkaClient(
    loggingService, KafkaConsumer, kafkaClientOptions, topics,
    maxConcurrentMsgProcessing, handlerProcessingTimeoutSec);

process.on('exit', async () => {
    loggingService.debug(() => 'Main - exiting process...');
    if (client) {
        await client.disconnectAsync();
    }
});

client.onMessage(async (msg: KafkaClientMessage): Promise<void> => {
    const timeToWait = +msg.key.split('-')[1];
    const messageInfo = `key:[${msg.key}]-topicPartition:[topic:${msg.kafka.topic}, partition:${msg.kafka.partition}, offset:${msg.kafka.offset}]`;
    loggingService.debug(() => `Main - message processing for ${timeToWait} ms ... -> ${messageInfo}`);
    await new Promise(resolve => setTimeout(resolve, timeToWait));
    loggingService.debug(() => `Main - message processing done after ${timeToWait} ms ... ->${messageInfo}`);
});

client.onError((err: Error): void => {
    loggingService.error(() => 'Main - onError', err);
});

client.onEventLog(
    (eventLog: KafkaLogEvent) => eventLog.severity !== KafkaLogSeverity.Debug,
    (eventLog: KafkaLogEvent): void => {
        loggingService.debug(() => `Main - onEventLog => ${KafkaLogUtil.toString(eventLog)}`);
    });

client.connectAsync();
