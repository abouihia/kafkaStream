package com.greeting_streams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;


public class GreetingStreamsApplication {

	 static void main(String[] args) {

		 SpringApplication.run(GreetingStreamsApplication.class, args);
	}

}
