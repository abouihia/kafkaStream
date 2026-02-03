package com.greeting_streams.domain;

import java.time.LocalDateTime;

public record GreetingRecord(String message, LocalDateTime  timeStamp) {
}
