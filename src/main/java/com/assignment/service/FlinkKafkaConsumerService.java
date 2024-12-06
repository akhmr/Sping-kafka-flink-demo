package com.assignment.service;

import org.springframework.stereotype.Service;

@Service
public class FlinkKafkaConsumerService {

//	// @Value("${spring.kafka.bootstrap-servers}")
//	private String bootstrapServers = "localhost:9092";
//
//	// @Value("${spring.kafka.topic.input}")
//	private String inputTopic = "test";
//
//	// @Value("${spring.kafka.topic.output}")
//	private String outputTopic = "test";
//
//	@PostConstruct
//	public void startFlinkJob() {
//		try {
//
//			var kafkaBroker = "localhost:9092";
//
////			var env = StreamExecutionEnvironment.getExecutionEnvironment();
////
////			var kafkaProps = new Properties();
////			kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
////			kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
////
////			Properties props = new Properties();
////			FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), props);
////
////			consumer.setStartFromEarliest();
////			
////			DataStream<String> dataStream  =env.addSource(consumer);
////			dataStream.print();
////			env.execute("Kafka Consumer Example");
//
//			// Create Flink execution environment
//			//StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//			
////			StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
////			env.setParallelism(1); // Set parallelism if needed
////
////			// Kafka properties
////			Properties kafkaProps = new Properties();
////			kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
////			kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
////
////			// Topic name
////			String topic = "test-flink";
////
////			// Create Kafka source
////			KafkaSource<String> kafkaConsumer = KafkaSource.<String>builder().setBootstrapServers(kafkaBroker)
////					.setTopics(topic).setGroupId("my-group").setProperties(kafkaProps)
////					.setStartingOffsets(OffsetsInitializer.earliest())
////					.setValueOnlyDeserializer(new SimpleStringSchema()).build();
////
////			// Create data stream from Kafka source
////			DataStream<String> stream = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "Kafka Source");
////			stream.print(); // Print the consumed messages to the standard output
////			env.execute("Kafka Flink Example");
//			
//			
////			 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
////
////		        // Create a stream from a sequence
////		        DataStream<Long> dataStream = env.fromSequence(1, 5); // Generates 1, 2, 3, 4, 5
////
////		        // Print the stream to the console
////		        dataStream.print();
////
////		        // Execute the Flink job
////		        env.execute("Simple Flink Job with fromSequence");
//			
//			String inputTopic = "test-flink";
//		    String outputTopic = "test";
//		    String consumerGroup = "my-group";
//		    String address = "localhost:9092";
//		    
////		    ClassLoader platformClassLoader = ClassLoader.getPlatformClassLoader();
////		    ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
////		    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
////		    ClassLoader threadClassLoader =  Thread.currentThread().getContextClassLoader();
////		    StreamExecutionEnvironment local = new StreamExecutionEnvironment((Configuration) env.getConfiguration(),threadClassLoader);
////		    local.setParallelism(5);
//		    
//		    Configuration conf = new Configuration();
//
//		    StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
//
//	        // Configure Kafka source properties
//	        Properties properties = new Properties();
//	        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//	        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
//
//	        // Create Kafka source
//	        KafkaSource<String> source = KafkaSource.<String>builder()
//	            .setBootstrapServers("localhost:9092")
//	            .setTopics("test-flink")
//	            .setGroupId("flink-consumer-group")
//	            .setStartingOffsets(OffsetsInitializer.earliest())
//	            .setValueOnlyDeserializer(new SimpleStringSchema())
//	            .build();
//
//	        // Add source to the environment
//	        DataStream<String> dataStream = environment.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//
//	        // Print the data stream to the console
//	        dataStream.print();
//
//	        // Execute the job
//	        environment.execute("Simple Flink Job with Kafka Source");
//
//
//
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
}
