package com.greeting_streams.exception;

import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public class StreamProcessorCustomErrorHandler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable throwable) {

        System.out.println("Exception in the application"+ throwable.getMessage());
        if (throwable instanceof StreamsException){
            var cause  = throwable.getCause();
            if( cause.getMessage().equals("Transient Error")){
                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
        }
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
