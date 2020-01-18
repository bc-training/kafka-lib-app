import { TopicPartition } from './kafka-message';

export class KafkaUtils {
    static getTopicPartitionStrg(topicPartition: TopicPartition): string {
        return `[topic=${topicPartition.topic}` +
            `, partition=${topicPartition.partition}` +
            `, offset=${topicPartition.offset}]`;
    }
}
