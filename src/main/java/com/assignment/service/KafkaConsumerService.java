package com.assignment.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
	
	
	@KafkaListener(topics = "evenTopic", groupId = "my-group")
    public void listenToEvenAge(String message) {
        System.out.println("Received message from test topic: " + message);
    }
	
	@KafkaListener(topics = "oddTopic", groupId = "my-group")
    public void listenToOddAge(String message) {
        System.out.println("Received message from oddTopic : " + message);
    }

}
