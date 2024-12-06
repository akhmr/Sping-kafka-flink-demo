package com.assignment.service;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.apache.kafka.common.serialization.StringSerializer;

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

    @Autowired
    private  KafkaProducerService kafkaProducerService;
    
    

    @PostConstruct
    public void runPipeline() {
        // Set up pipeline options
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(DirectRunner.class);

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);
        
      //  RouteMessageDoFn routeMessageDoFn = new RouteMessageDoFn();

        pipeline
            // Read messages from the input Kafka topic
            .apply(KafkaIO.<String, String>read()
                .withBootstrapServers(bootstrapServers)
                .withTopic(inputTopic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata())
            .apply(MapElements
                .into(TypeDescriptor.of(String.class))
                .via((kafkaRecord) -> kafkaRecord.getValue()))
            .apply(ParDo.of(new FilterEvenLengthDoFn()))
            .apply(Filter.by((String message) -> message != null))
           // .apply(ParDo.of(new FilterEvenLengthDoFn()))
            .apply(KafkaIO.<String, String>write()
                    .withBootstrapServers(bootstrapServers)
                    .withTopic(evenTopic)  // You might decide to handle different topics here
                    .withKeySerializer(StringSerializer.class)
                    .withValueSerializer(StringSerializer.class)
                    .values());  // Wr
                
        // Run the pipeline
        new Thread(() -> pipeline.run().waitUntilFinish()).start();
    }

    // Static DoFn for filtering even-length messages
    private static class FilterEvenLengthDoFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String message, OutputReceiver<String> out) {
        	
        	System.out.println("Received message from Kafka: "+message);
            if (message != null ) {
                out.output(message.substring(0, 3));
            }
        }
    }

//    // Static DoFn for routing messages to Kafka topics
//    private static class RouteMessageDoFn extends DoFn<String, Void> {
//        @ProcessElement
//        public void processElement(@Element String message) {
//            if (message.length() % 2 == 0) {
//                 //kafkaProducerService.sendToKafka(evenTopic, message);
//                System.out.println("Sent to even topic: " + message);
//            } else {
//                // kafkaProducerService.sendToKafka(oddTopic, message);
//                System.out.println("Sent to odd topic: " + message);
//            }
//        }
//    }
    
    private static class RouteMessageDoFn extends DoFn<String, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element String message, OutputReceiver<KV<String, String>> out) {
            if (message != null) {
                // Split the logic to decide what key-value to output
                if (message.length() % 2 == 0) {
                    // Send to 'even' topic with a specific key-value
                    out.output(KV.of("evenKey", message));  // Key-value pair for even messages
                } else {
                    // Send to 'odd' topic with a different key-value
                    out.output(KV.of("oddKey", message));  // Key-value pair for odd messages
                }
            }
        }
    }
    
   
}
