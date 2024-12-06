package com.assignment.service;

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

    // Define TupleTags for the outputs
    private static final TupleTag<String> EVEN_TAG = new TupleTag<String>() {};
    private static final TupleTag<String> ODD_TAG = new TupleTag<String>() {};

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

        // Filter even-length messages
        PCollection<String> evenMessages = messages.apply("FilterEvenLength", ParDo.of(new EvenLengthFilterFn()));

        // Filter odd-length messages
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
            if (message != null && message.length() % 2 == 0) {
                out.output(message);
            }
        }
    }

    // Static DoFn for filtering odd-length messages
    private static class OddLengthFilterFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String message, OutputReceiver<String> out) {
            if (message != null && message.length() % 2 != 0) {
                out.output(message);
            }
        }
    }

    // Static DoFn for splitting messages into even and odd based on their length
    private static class SplitMessagesDoFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String message, MultiOutputReceiver out) {
            if (message.length() % 2 == 0) {
                out.get(EVEN_TAG).output(message);  // Output to 'even' side
            } else {
                out.get(ODD_TAG).output(message);   // Output to 'odd' side
            }
        }
    }
}
