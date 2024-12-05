package com.assignment.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.assignment.service.KafkaProducerService;

@RestController
public class DataController {
	
	@Autowired
	private KafkaProducerService kafkaProducerService;
	
	
	
	@GetMapping("/send")
    public String sendMessage() {
		kafkaProducerService.sendMessage("test", "Hello from Spring Boot!");
        return "Message sent successfully!";
    }

	@GetMapping("/health")
	public String heatlh() {
		return "ok";
	}
	
	

}
