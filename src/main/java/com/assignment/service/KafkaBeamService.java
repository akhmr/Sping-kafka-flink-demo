package com.assignment.service;

import java.util.Properties;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;
import org.apache.beam.runners.direct.DirectRunner;

import jakarta.annotation.PostConstruct;

@Service
public class KafkaBeamService {

    private static final String INPUT_TOPIC = "test";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @PostConstruct
    public void runPipeline() {
        // Create the pipeline
    	  PipelineOptions options = PipelineOptionsFactory.create();
          options.setRunner(DirectRunner.class); 

          // Create the pipeline
          Pipeline pipeline = Pipeline.create(options);

          

        pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(BOOTSTRAP_SERVERS)
                .withTopic(INPUT_TOPIC)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata())
                .apply(MapElements
                        .into(TypeDescriptor.of(String.class))
                        .via((message) -> {
                            // Print received message
                            System.out.println("Received message and handling through beam: " + message.getValue());
                            return message.getValue();
                        }))
                .apply(Filter.by((String message) -> {
                    // Parse message as JSON or assume a format where the age is included
                    try {
                        int age = Integer.parseInt(message);
                        
                        System.out.println("Age "+age);// Replace with proper parsing if needed
                        return age > 20;
                    } catch (NumberFormatException e) {
                        return false; // Ignore malformed messages
                    }
                }));
//                .apply(FlatMapElements
//                        .into(TypeDescriptor.of(String.class))
//                        .via((String message) -> {
//                            // Push the filtered message back to Kafka
//                            sendToKafka(message);
//                            return Collections.singletonList(message);
//                        }));

        // Run the pipeline and wait for it to finish
        pipeline.run().waitUntilFinish();
    }

    private void sendToKafka(String message) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>(OUTPUT_TOPIC, message));
        producer.close();
    }
}