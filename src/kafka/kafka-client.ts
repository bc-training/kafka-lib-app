import { Service } from 'typedi';
import { KafkaClientMessage, KafkaMessageUtil } from './kafka-message';
import { KafkaLogEvent } from './kafka-log';
import { KafkaConsumer } from 'node-rdkafka';
import { KafkaOptionKeys } from './kafka-option-keys';
import { KafkaUtils } from './kafka-util';
import { LoggingService } from '../core/logging-service';

const uuid = require('uuid/v4');
const Kafka = require('node-rdkafka');

/**
 * RdKafka Consumer Class Constructor type definition
 **/
export type RdKafkaConsumerClassConstructor = new (conf: any, topicConf: any) => any;

/**
 * Kafka client handlers : Message, Error, LogEvent
 **/
export type MessageEventHandler = (msg: KafkaClientMessage) => Promise<void>;
export type ErrorEventHandler = (err: Error) => void;
export type LogEventHandler = (log: KafkaLogEvent) => void;

/**
 * Kafka client
 *
 * This class is a wrapper on top of kafka libs
 * npm package : https://github.com/Blizzard/node-rdkafka
 * C++ library : https://github.com/edenhill/librdkafka
 **/
@Service()
export class KafkaClient {

    private static readonly MAX_CONCURRENT_HANDLER_PROCESSING = 250;
    private static readonly HANDLER_PROCESSING_TIMEOUT_SEC = 30;

    private _connected = false;                       // Kafka Topic connection status
    private _rdkafkaConsumer: KafkaConsumer;
    private _pendingHandlerProcessCount = 0;
    private _messageHandler: MessageEventHandler = <any>null;
    private _clientIdentifier = '';

    constructor(
        private loggingService: LoggingService,
        rdKafkaConsumerCtor: RdKafkaConsumerClassConstructor,
        private rdKafkaOptions: any,
        private topics: Array<string>,
        private maxConcurrentMsgProcessing = KafkaClient.MAX_CONCURRENT_HANDLER_PROCESSING,
        private handlerProcessingTimeoutSec = KafkaClient.HANDLER_PROCESSING_TIMEOUT_SEC) {
        if (maxConcurrentMsgProcessing <= 0) {
            this.maxConcurrentMsgProcessing = KafkaClient.MAX_CONCURRENT_HANDLER_PROCESSING;
        }
        if (handlerProcessingTimeoutSec <= 0) {
            this.handlerProcessingTimeoutSec = KafkaClient.HANDLER_PROCESSING_TIMEOUT_SEC;
        }
        this.rdKafkaOptions = this.updateOptions(rdKafkaOptions);
        this._rdkafkaConsumer = new rdKafkaConsumerCtor(this.rdKafkaOptions, {});
        this.loggingService.debug(() =>
            `${this.clientIdentifier} Successfully built` +
            `\r\ntopics:[${this.topics.join(', ')}]` +
            `\r\nmaxConcurrentMsgProcessing:${maxConcurrentMsgProcessing}` +
            `\r\n${JSON.stringify(this.rdKafkaOptions, null, 2)}`);
    }

    /**
     * Connect the client to a Kafka topic
     *
     * If the client is already connected (the process continues silently).
     **/
    async connectAsync(): Promise<this> {
        if (this._connected) {
            this.loggingService.warn(() => `${this.clientIdentifier} Already connected. Continuing silently...`);
            return this;
        }
        this.subscribeToKafkaEvents();
        return this;
    }

    /**
     * Disconnect the Kafka client
     *
     * If the client is already disconnected (the process continues silently).
     **/
    async disconnectAsync(): Promise<this> {
        if (!this._connected) {
            this.loggingService.warn(() => `${this.clientIdentifier} Already disconnected. Continuing silently...`);
            return this;
        }
        this._rdkafkaConsumer.disconnect((err: Error, res: any) => {
            if (err) {
                this.loggingService.error(() => `${this.clientIdentifier} Disconnection failure`, err);
                this._connected = false;
                throw err;
            }
            this._connected = false;
            this.loggingService.debug(() => `${this.clientIdentifier} Disconnection succeeded`);
        });
        return this;
    }

    get isConnected(): boolean {
        return this._connected;
    }

    /**
     Messages handlers :
     - onMessage  - Kafka client publishes a KafkaClientMessage
     - onError    - Kafka client returns an Error
     - onEventLog - Kafka client raise a KafkaLogEvent (that can be filtered)
     **/
    onMessage(handler: MessageEventHandler): void {
        this._messageHandler = handler;
    }

    onError(handler: ErrorEventHandler): void {
        this._rdkafkaConsumer.on('event.error', (err: Error) => handler(err));
    }

    onEventLog(filter: (event: KafkaLogEvent) => boolean, handler: LogEventHandler): void {
        this._rdkafkaConsumer.on('event.log', (log: any) => {
            if (filter && (filter(log) === true)) {
                handler(log);
            }
        });
    }

    /**
     * Unique identifer used for logging purpose only.
     * Cached in this._clientIdentifier
     */
    private get clientIdentifier(): string {
        return this._clientIdentifier
            ? this._clientIdentifier
            : `${KafkaClient.name}::${this.rdKafkaOptions[KafkaOptionKeys.CLIENT_ID]}-${uuid()}-`;
    }

    private subscribeToKafkaEvents(): void {
        this._rdkafkaConsumer.on('ready', () => {
            this.loggingService.info(() => `${this.clientIdentifier} Ready & subscribed to topic(s) [${this.topics.join(', ')}] ...`);
            this._rdkafkaConsumer.subscribe(this.topics);
            this.startConsumeLoopAsync();
        });

        this._rdkafkaConsumer.connect({}, (err: Error, res: any) => {
            if (err) {
                this.loggingService.error(() => `${this.clientIdentifier} Connection failure`, err);
                throw err;
            } else {
                this._connected = true;
                this.loggingService.info(() => `${this.clientIdentifier} Connection succeeded`);
                this.loggingService.debug(() => `${this.clientIdentifier} Connection result`, res);
            }
        });
    }

    private updateOptions(rdKafkaOptions: any): any {
        this.loggingService.debug(() => `${this.clientIdentifier} Update options...`);
        return {
            ...rdKafkaOptions,
            'debug': 'all',
            'offset_commit_cb': (err: any, topicPartitions: any) => {
                if (err) {
                    this.loggingService.error(() => `${this.clientIdentifier} Error occurred when calling Offset_commit_cb`, err, topicPartitions);
                } else {
                    this.loggingService.debug(() => `${this.clientIdentifier} Offset_commit_cb invoked`, topicPartitions);
                }
            },
            'rebalance_cb': (err: any, assignment: any) => {
                try {
                    if (err.code === Kafka.CODES.ERRORS.ERR__ASSIGN_PARTITIONS) {
                        this.loggingService.info(() => `${this.clientIdentifier} Rebalance_cb invoked-ERR__ASSIGN_PARTITIONS`, assignment);
                        this._rdkafkaConsumer.assign(assignment);
                    } else if (err.code === Kafka.CODES.ERRORS.ERR__REVOKE_PARTITIONS) {
                        this.loggingService.info(() => `${this.clientIdentifier} Rebalance_cb invoked-ERR__REVOKE_PARTITIONS`, assignment);
                        this._rdkafkaConsumer.unassign();
                    } else {
                        this.loggingService.error(() => `${this.clientIdentifier} Error occurred when calling Rebalance_cb`, err, assignment);
                    }
                } catch (err) {
                    this.loggingService.error(() => `${this.clientIdentifier} Unexpected error in rebalance_cb will be ignored.`, err);
                }
            }
        };
    }

    private async startConsumeLoopAsync(): Promise<void> {
        this.loggingService.info(() => `${this.clientIdentifier} Start consuming messages in loop from topics [${this.topics.join(', ')}] ...`);
        while (true) {
            try {
                const consumerCount = this.maxConcurrentMsgProcessing - this._pendingHandlerProcessCount;
                const data: any[] = await this.consumeMessagesAsync(consumerCount);
                const shouldWait = (consumerCount - data.length) === 0;
                await this.handleMessagesAsync(data, shouldWait);
            } catch (err) {
                this.loggingService.error(() => `${this.clientIdentifier} Error occurred in startConsumeLoop`, err);
            }
        }
    }

    private consumeMessagesAsync(consumerCount: number): Promise<any[]> {
        return new Promise((resolve, reject) => {
            this._rdkafkaConsumer.consume(consumerCount, (err: any, messages: any) => {
                if (err) {
                    return reject(err);
                }
                resolve(messages);
            });
        });
    }

    private timeout(ms: number, handlerPromise: Promise<any>): Promise<any> {
        let timeoutId = <any>null;
        const timeoutPromise = new Promise((_, reject) => {
            timeoutId = setTimeout(() => reject(Error(`Operation time out after ${ms} ms`)), ms);
        });
        return Promise.race([timeoutPromise, handlerPromise])
            .finally(() => clearTimeout(timeoutId));
    }

    private async handleMessagesAsync(messages: any[], shouldWait: boolean): Promise<void> {
        if (messages && messages.length > 0) {
            this.loggingService.debug(() => `${this.clientIdentifier} Got ${messages.length} message(s) to handle`);
            const pendingHandlerPromises = new Array<Promise<any>>();
            for (let index = 0; index < messages.length; index++) {
                this.loggingService.debug(() => `PendingHandlerProcessCount incremented to ${this._pendingHandlerProcessCount}`);
                pendingHandlerPromises.push(new Promise(async (resolve, reject) => {
                    let kafkaClientMessage: KafkaClientMessage = <any>null;
                    try {
                        const rdKafkaMsg = messages[index];
                        kafkaClientMessage = KafkaMessageUtil.createClientMessageFromRdKafkaMsg(rdKafkaMsg);
                        this.loggingService.debug(() => `${this.clientIdentifier} Received key:${kafkaClientMessage.key}` +
                            ` - ${KafkaUtils.getTopicPartitionStrg(kafkaClientMessage.kafka)}`);

                        // If the handler does not finish its working after the this.handlerProcessingTimeoutSec timeout
                        // - the handler become ghost
                        // - the slot used for it for back-pressure management in made free
                        // - ths async operation is still working (E.g.microService processing is still executing)
                        this._pendingHandlerProcessCount++;
                        await this.timeout(this.handlerProcessingTimeoutSec * 1000, this._messageHandler(kafkaClientMessage));
                    } catch (err) {
                        this.loggingService.error(() => `${this.clientIdentifier} Error ocurred when calling [handleMessages] key:${kafkaClientMessage.key}`, err);
                    } finally {
                        this._pendingHandlerProcessCount--;
                        this.loggingService.debug(() => `PendingHandlerProcessCount decremented to ${this._pendingHandlerProcessCount}`);
                        resolve();
                    }
                }));
            }

            /**
             * We reach the limit of concurrent handlers ( fixed by this.maxConcurrentMsgProcessing)
             * As soon as an handler finishes we will consume new message from the Kafka topic
             **/
            if (shouldWait) {
                this.loggingService.warn(() => `${this.clientIdentifier} Back pressure locked limit of ${this.maxConcurrentMsgProcessing} is reached ` +
                    `-> waiting one handler to finish...`);

                await Promise.race(pendingHandlerPromises);

                this.loggingService.warn(() => `${this.clientIdentifier} Back pressure unlocked ` +
                    `-> one or more handler finish their work.`);
            }
        }
    }
}
