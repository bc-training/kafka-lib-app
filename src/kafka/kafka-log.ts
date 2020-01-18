/**
 * Kafka Message.
 *
 * This is the representation of a Kafka Log record
 *
 **/
export interface KafkaLogEvent {
    severity: KafkaLogSeverity;
    fac: string;
    message: string;
}

/**
 * KafkaLog Severity.
 *
 * This is the representation of a Kafka Log record severity
 * conforms to syslog(3) severities
 * See http://www.solarwinds.com/documentation/kiwi/help/syslog/index.html?protocol_levels.htm for more info
 **/
export enum KafkaLogSeverity {
    Emergency = 0,       // system is unusable
    Alert = 1,           // action must be taken immediately
    Critical = 2,        // critical conditions
    Error = 3,           // error conditions
    Warning = 4,         // warning conditions
    Notice = 5,          // normal but significant condition
    Informational = 6,   // informational messages
    Debug = 7            // debug-level messages
}

/**
 * KafkaLog  Helper class.
 *
 * Convert a KafkaLog to a string representation.
 **/
export class KafkaLogUtil {
    static toString(event: KafkaLogEvent): string {
        return `${event.severity}-${event.fac}-${event.message}`;
    }
}
