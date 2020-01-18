/**
 * RdKafka Consumer message.
 *
 * This is the representation of a message read from Kafka.
 * As received by the RdKafka library
 **/
export interface RdKafkaConsumerMessage {
    value: Buffer;        // message buffer from Kafka
    size: number;         // size of the message, in bytes
    topic: string;        // name of the topic the message comes from
    offset: number;       // offset the message was read from
    partition: number;    // partition on the topic the message was on
    key: any;             // message key
    timestamp: number;    // message timestamp
}

/**
 * RdKafka Topic partition.
 *
 * This is the representation of a Kafka topic partition.
 *
 **/
export interface TopicPartition {
    topic: string;      // topic the message comes from
    offset: number;     // offset the message was read from
    partition: number;  // partition the message was on
}

/**
 * Couchbase Document metadata.
 *
 * These are teg metadata associated with a Couchbase document.
 *
 **/
export interface CouchbaseMeta {
    event: 'mutation' | 'deletion' | 'expiration';  // events types
    cas: string;      // document CAS value (Check And Set)
    bySeqno: number;  //  couchbase seq number
    revSeqno: number; // document revision number
    bucket: string;   // name od the bucket the doc belongs to
}

/**
 * Kafka Message.
 *
 * This is the representation of a Kafka message.
 *
 **/
export interface KafkaClientMessage {
    timestamp: number;        // timestamp of message creation
    key: string;              // key of the message if present
    kafka: TopicPartition;    // kafka partition
    redelivered: boolean;     // message has been already delivered
    couchbase?: CouchbaseMeta; // couchbase document metadata
    content?: string;         // optional message contents
}

/**
 * Kafka Message Utility class
 *
 * Utility class used to transform RdKafkaConsumerMessage to KafkaClientMessage
 *
 **/
export class KafkaMessageUtil {

    static createClientMessageFromRdKafkaMsg(rdKafkaMsg: RdKafkaConsumerMessage): KafkaClientMessage {

        const encoding = 'utf8';
        let value: any = rdKafkaMsg.value.toString(encoding);
        try {
            value = JSON.parse(value);
            if (value.schema) {
                return KafkaMessageUtil.createClientMessageFromCouchbaseConnect(rdKafkaMsg, value.payload);
            } else {
                return KafkaMessageUtil.createClientMessageFromKafkaProvider(rdKafkaMsg, value);
            }
        } catch{
            // Error can be thrown if we decided that all message must be JSON message
            return KafkaMessageUtil.createClientMessageFromKafkaProvider(rdKafkaMsg, value, false);
        }
    }

    static createClientMessageFromCouchbaseConnect(rdKafkaMsg: RdKafkaConsumerMessage, value: any): KafkaClientMessage {
        try {
            const encoding = 'utf8';
            const key = rdKafkaMsg.key ? rdKafkaMsg.key.toString(encoding) : '';
            const clientMessage: KafkaClientMessage = {
                timestamp: rdKafkaMsg.timestamp,
                key: key,
                redelivered: false,
                kafka: {
                    topic: rdKafkaMsg.topic,
                    offset: rdKafkaMsg.offset,
                    partition: rdKafkaMsg.partition
                },
                couchbase: {
                    event: value.event,
                    cas: value.cas,
                    bySeqno: value.bySeqno,
                    revSeqno: value.revSeqno,
                    bucket: value.bucket
                }
            };

            if (value.event === 'mutation' || value.event === 'deletion') {
                clientMessage.content = JSON.parse(Buffer.from(value.content, 'base64').toString(encoding));
            }
            return clientMessage;
        } catch (ex) {
            console.error('Something went wrong with the message format !', ex);
            throw new Error('invalid message');
        }
    }

    private static createClientMessageFromKafkaProvider(rdKafkaMsg: RdKafkaConsumerMessage, value: any, isJson = true): KafkaClientMessage {
        const content = rdKafkaMsg.value.toString('utf8');
        return {
            timestamp: rdKafkaMsg.timestamp,
            key: content,
            content: isJson ? JSON.parse(value) : value,
            redelivered: false,
            kafka: {
                topic: rdKafkaMsg.topic,
                offset: rdKafkaMsg.offset,
                partition: rdKafkaMsg.partition
            }
        };
    }
}
