/**
 * Kafka Options Keys
 *
 * Describe the Kafka option keys.
 * See http://kafka.apache.org/documentation.html#consumerconfigs for more info
 *
 **/
export class KafkaOptionKeys {
    static METADATA_BROKER_LIST = 'metadata.broker.list';
    static CLIENT_ID = 'client.id';
    static GROUP_ID = 'group.id';

    static FETCH_WAIT_MAX_MS = 'fetch.wait.max.ms';             // Default : 500
    static FETCH_MIN_BYTES = 'fetch.min.bytes';                 // Default : 1
    static FETCH_MAX_BYTES = 'fetch.max.bytes';                 // Default : 52428800

    static MAX_POLL_INTERVAL_MS = 'max.poll.interval.ms';       // Default : 300000
    static MAX_POLL_RECORDS = 'max.poll.records';               // Default : 500

    static ENABLE_AUTO_COMMIT = 'enable.auto.commit';          // Default: true
    static AUTO_COMMIT_INTERVAL_MS = 'auto.commit.interval.ms'; // Default : 5000

    static REQUEST_TIMEOUT_MS = 'request.timeout.ms';           // Default : 30000

    static HEARTBEAT_INTERVAL_MS = 'heartbeat.interval.ms';     // Default : 3000
    static SESSION_TIMEOUT_MS = 'session.timeout.ms';           // Default : 10000
    static AUTO_OFFSET_RESET = 'auto.offset.reset';             // Default latest, allowed : [latest | earliest | none]

    static ENABLE_AUTO_OFFSET_STORE = 'enable.auto.offset.store';

    static SECURITY_PROTOCOL = 'security.protocol';             // Default: plaintext, allowed: plaintext | ssl | sasl_plaintext | sasl_ssl
    static SASL_MECHANISM = 'sasl.mechanism';                   // Default: GSSAPI, allowed:  GSSAPI | PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
    static SASL_USERNAME = 'sasl.username';
    static SASL_PASSWORD = 'sasl.password';
}
