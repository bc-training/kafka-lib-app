import { KafkaOptionKeys } from './kafka/kafka-option-keys';
import { KafkaClient } from './kafka/kafka-client';
import { LoggingService } from './core/logging-service';
import { KafkaClientMessage } from './kafka/kafka-message';
import { KafkaConsumer } from 'node-rdkafka';

export class TopicConsumer {
    private loggingService = new LoggingService();

    start(): TopicConsumer {
        const kafkaClientOptions = {
            [KafkaOptionKeys.METADATA_BROKER_LIST]: 'localhost:9092',
            [KafkaOptionKeys.CLIENT_ID]: 'rqm-client',
            [KafkaOptionKeys.GROUP_ID]: 'test-group',
            [KafkaOptionKeys.ENABLE_AUTO_COMMIT]: true,
            [KafkaOptionKeys.AUTO_COMMIT_INTERVAL_MS]: 3000,
            [KafkaOptionKeys.HEARTBEAT_INTERVAL_MS]: 3000,
            [KafkaOptionKeys.SESSION_TIMEOUT_MS]: 7000
        };
        const topic = 'rqm-entity';
        const maxConcurrentMsgProcessing = 250;
        const handlerProcessingTimeoutSec = 60;
        const client = new KafkaClient(this.loggingService, KafkaConsumer, kafkaClientOptions, [topic],
            maxConcurrentMsgProcessing, handlerProcessingTimeoutSec);

        process.on('exit', async () => {
            if (client) {
                await client.disconnectAsync();
            }
        });

        client.onMessage(async (msg: KafkaClientMessage): Promise<void> => {
            // if (msg.content) {
            //     const document = msg.content; // as any as Document<any>;
            //     this.loggingService.debug(() => JSON.stringify(document, null, 2));
            // } else {
            this.loggingService.debug(() => JSON.stringify(msg, null, 2));
            // }
        });

        client.onError((err: Error): void => {
            this.loggingService.error(() => 'Kafka error has occurred', err);
        });
        client.connectAsync();
        return this;
    }
}
