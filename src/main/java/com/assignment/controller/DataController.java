package com.assignment.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DataController {

	@RestController
	public class HealthController {

		@GetMapping("/health")
		public String heatlh() {
			return "ok";
		}

	}

}
