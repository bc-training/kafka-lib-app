export type LogMsgType = () => string;

export class LoggingService {

    trace(obj: LogMsgType, ...optionalParams: any[]): void {
        console.log(obj(), optionalParams);
    }
    debug(obj: LogMsgType, ...optionalParams: any[]): void {
        console.log(obj(), optionalParams);
    }
    info(obj: LogMsgType, ...optionalParams: any[]): void {
        console.log(obj(), optionalParams);
    }
    warn(obj: LogMsgType, ...optionalParams: any[]): void {
        console.log(obj(), optionalParams);
    }
    error(obj: LogMsgType, ...optionalParams: any[]): void {
        console.error(obj(), optionalParams);
    }
}
