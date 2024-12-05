package com.assignment.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public String sendMessage(String topic,String msg) {
		kafkaTemplate.send(topic, msg);
		return "Message sent successfully!";
	}

}
