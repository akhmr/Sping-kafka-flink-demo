package com.assignment.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
	
	@KafkaListener(topics = "test", groupId = "my-group")
    public void listen(String message) {
        System.out.println("Received message from test topic: " + message);
    }

}
