package com.example;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Simple Redpanda Avro consumer that consumes the last N messages from a topic.
 */
public class AvroConsumer {
    // Static configuration variables
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092";
    private static final String TOPIC_NAME = "test-topic";
    private static final String USERNAME = "superuser";
    private static final String PASSWORD = "secretpassword";
    private static final String AVRO_SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String TRUSTSTORE_PATH = null; // e.g., "/path/to/truststore.jks"
    private static final String TRUSTSTORE_PASSWORD = null; // e.g., "truststore-password"
    private static final String SECURITY_PROTOCOL = "SASL_SSL";
    private static final String SASL_MECHANISM = "SCRAM-SHA-512";
    private static final String SASL_JAAS_CONFIG = 
        String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
            USERNAME, PASSWORD);
    
    public static void main(String[] args) {
        // Get number of messages to consume from command line argument
        int numMessages = 10; // default
        if (args.length > 0) {
            try {
                numMessages = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid number of messages: " + args[0] + ". Using default: 10");
            }
        }
        
        System.out.println("==========================================");
        System.out.println("Redpanda Avro Consumer");
        System.out.println("==========================================");
        System.out.println("Bootstrap servers: " + BOOTSTRAP_SERVERS);
        System.out.println("Topic: " + TOPIC_NAME);
        System.out.println("Schema Registry URL: " + AVRO_SCHEMA_REGISTRY_URL);
        System.out.println("Security protocol: " + SECURITY_PROTOCOL);
        System.out.println("SASL mechanism: " + SASL_MECHANISM);
        System.out.println("Number of messages to consume: " + numMessages);
        System.out.println("==========================================\n");
        
        KafkaConsumer<String, Object> consumer = null;
        
        try {
            // Create consumer properties
            Properties props = createConsumerProperties();
            
            System.out.println("Creating KafkaConsumer with Avro deserializer...");
            consumer = new KafkaConsumer<>(props);
            
            System.out.println("Subscribing to topic: " + TOPIC_NAME);
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            
            // Wait for partition assignment
            System.out.println("Waiting for partition assignment...");
            consumer.poll(Duration.ofSeconds(5));
            
            // Get assigned partitions
            List<TopicPartition> partitions = consumer.assignment().stream()
                .collect(Collectors.toList());
            
            if (partitions.isEmpty()) {
                System.out.println("No partitions assigned. Exiting.");
                return;
            }
            
            System.out.println("Assigned partitions: " + partitions);
            
            // Seek to end to get the latest offsets
            System.out.println("Seeking to end of partitions...");
            consumer.seekToEnd(partitions);
            
            // Get end offsets and seek to position for last N messages
            System.out.println("Calculating positions for last " + numMessages + " messages...");
            for (TopicPartition partition : partitions) {
                long endOffset = consumer.position(partition);
                long startOffset = Math.max(0, endOffset - numMessages);
                System.out.println("Partition " + partition.partition() + 
                    ": end offset=" + endOffset + ", seeking to offset=" + startOffset);
                consumer.seek(partition, startOffset);
            }
            
            System.out.println("\nConsuming messages...\n");
            
            // Consume messages
            int messageCount = 0;
            long endTime = System.currentTimeMillis() + 30000; // 30 second timeout
            
            while (messageCount < numMessages && System.currentTimeMillis() < endTime) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(5));
                
                if (records.isEmpty()) {
                    System.out.println("No more messages available.");
                    break;
                }
                
                for (ConsumerRecord<String, Object> record : records) {
                    if (messageCount >= numMessages) {
                        break;
                    }
                    
                    System.out.println("----------------------------------------");
                    System.out.println("Message #" + (messageCount + 1));
                    System.out.println("Partition: " + record.partition());
                    System.out.println("Offset: " + record.offset());
                    System.out.println("Key: " + record.key());
                    System.out.println("Timestamp: " + record.timestamp());
                    System.out.println("Value (Avro): " + record.value());
                    System.out.println("Value Type: " + 
                        (record.value() != null ? record.value().getClass().getName() : "null"));
                    System.out.println("----------------------------------------\n");
                    
                    messageCount++;
                }
            }
            
            System.out.println("==========================================");
            System.out.println("Successfully consumed " + messageCount + " message(s)");
            System.out.println("==========================================");
            
        } catch (Exception e) {
            System.out.println("==========================================");
            System.out.println("FAILED: Error consuming messages");
            System.out.println("==========================================");
            System.out.println("Exception Type: " + e.getClass().getName());
            System.out.println("Message: " + e.getMessage());
            System.out.println("\nFull Stack Trace:");
            e.printStackTrace();
            System.exit(1);
            
        } finally {
            if (consumer != null) {
                try {
                    System.out.println("\nClosing consumer...");
                    consumer.close();
                    System.out.println("Consumer closed successfully.");
                } catch (Exception e) {
                    System.out.println("Warning: Error while closing consumer:");
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * Creates properties for Kafka Consumer with Avro deserializer and SASL_SSL authentication.
     */
    private static Properties createConsumerProperties() {
        Properties props = new Properties();
        
        // Bootstrap servers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        
        // Consumer group ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-consumer-group");
        
        // Key deserializer (String)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // Value deserializer (Avro)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        
        // Schema Registry URL
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, AVRO_SCHEMA_REGISTRY_URL);
        
        // Auto offset reset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // Session timeout
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        
        // Heartbeat interval
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        
        // Request timeout
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        
        // Metadata fetch timeout
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        
        // SSL/Trust Store configuration
        if (TRUSTSTORE_PATH != null && !TRUSTSTORE_PATH.isEmpty()) {
            // Trust store location
            props.put("ssl.truststore.location", TRUSTSTORE_PATH);
            
            // Trust store password
            if (TRUSTSTORE_PASSWORD != null && !TRUSTSTORE_PASSWORD.isEmpty()) {
                props.put("ssl.truststore.password", TRUSTSTORE_PASSWORD);
            }
        }
        
        // SASL configuration
        props.put("security.protocol", SECURITY_PROTOCOL);
        props.put("sasl.mechanism", SASL_MECHANISM);
        props.put("sasl.jaas.config", SASL_JAAS_CONFIG);
        
        return props;
    }
}

