package com.assignment.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.assignment.service.KafkaProducerService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
public class DataController {
	
	@Autowired
	private KafkaProducerService kafkaProducerService;
	
	private final ObjectMapper objectMapper = new ObjectMapper();
	
	
	
	@GetMapping("/send")
    public String sendMessage() throws JsonProcessingException {
		
		Person p = new Person();
		p.setName("Arvind");
		p.setAddress("gurgaon");
		p.setDateOfBirth("20/12/1985");
		
		kafkaProducerService.sendToKafka("oddTopic", objectMapper.writeValueAsString(p));
        return "Message sent successfully!";
    }

	@GetMapping("/health")
	public String heatlh() {
		return "ok";
	}
	
	

}
