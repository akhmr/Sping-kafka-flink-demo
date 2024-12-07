package com.assignment.service;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.assignment.controller.Person;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;

@Service
public class KafkaBeamService {

    @Value("${spring.kafka.topic.input}")
    private String inputTopic;

    @Value("${spring.kafka.topic.even}")
    private String evenTopic;

    @Value("${spring.kafka.topic.odd}")
    private String oddTopic;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @PostConstruct
    public void runPipeline() {
        // Set up pipeline options
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        // Read messages from the input Kafka topic
        PCollection<String> messages = pipeline
            .apply(KafkaIO.<String, String>read()
                .withBootstrapServers(bootstrapServers)
                .withTopic(inputTopic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata())
            .apply(MapElements
                .into(TypeDescriptor.of(String.class))
                .via((kafkaRecord) -> kafkaRecord.getValue()));

        PCollection<String> evenMessages = messages.apply("FilterEvenLength", ParDo.of(new EvenLengthFilterFn()));

        PCollection<String> oddMessages = messages.apply("FilterOddLength", ParDo.of(new OddLengthFilterFn()));

        // Write even-length messages to the 'even' Kafka topic
        evenMessages.apply("WriteEvenMessagesToKafka", KafkaIO.<String, String>write()
            .withBootstrapServers(bootstrapServers)
            .withTopic(evenTopic)
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(StringSerializer.class)
            .values());

        // Write odd-length messages to the 'odd' Kafka topic
        oddMessages.apply("WriteOddMessagesToKafka", KafkaIO.<String, String>write()
            .withBootstrapServers(bootstrapServers)
            .withTopic(oddTopic)
            .withKeySerializer(StringSerializer.class)
            .withValueSerializer(StringSerializer.class)
            .values());

        // Run the pipeline
        new Thread(() -> pipeline.run().waitUntilFinish()).start();
    }

    // Static DoFn for filtering even-length messages
    private static class EvenLengthFilterFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String message, OutputReceiver<String> out) {
        	
        	ObjectMapper objectMapper = new ObjectMapper();
        	try {
				Person person = objectMapper.readValue(message, Person.class);
				System.out.println("Person = "+person.toString());
				
				int age = calculateAge(person.getDateOfBirth());
                
				if (age != -1 && age % 2 == 0) {
	                out.output("Age is even and age is ="+age);
	            }
			} catch (Exception e) {
				e.printStackTrace();
				out.output("exception occured");
			}
            
        }

    }
    
    private static int calculateAge(String dateOfBirth) {
		try {
            // Define the date format (adjust as per your input date format)
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
            LocalDate dob = LocalDate.parse(dateOfBirth, formatter);
            return Period.between(dob, LocalDate.now()).getYears();
        } catch (Exception e) {
            System.err.println("Error parsing date of birth: " + e.getMessage());
            return -1; // Return -1 if parsing fails
        }
	}

    // Static DoFn for filtering odd-length messages
    private static class OddLengthFilterFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String message, OutputReceiver<String> out) {
        	ObjectMapper objectMapper = new ObjectMapper();
        	try {
				Person person = objectMapper.readValue(message, Person.class);
				System.out.println("Person = "+person.toString());
				
				int age = calculateAge(person.getDateOfBirth());
                
				if (age != -1 && age % 2 != 0) {
	                out.output("Age is odd and age is ="+age);
	            }
			} catch (Exception e) {
				e.printStackTrace();
				out.output("exception occured");
			}
        }
    }

}