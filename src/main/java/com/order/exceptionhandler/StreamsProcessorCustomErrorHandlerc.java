package com.order.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class StreamsProcessorCustomErrorHandlerc implements StreamsUncaughtExceptionHandler {

    @Override
    public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse handle(Throwable exception) {
        log.error("Exception in the Application : {} ",exception.getMessage(), exception);
        if(exception instanceof StreamsException){
            var cause = exception.getCause();
            if(cause.getMessage().equals("Transient Error")){
                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
        }
        log.error("Shutdown the client");
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
