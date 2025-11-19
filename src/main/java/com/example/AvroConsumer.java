package com.example;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Simple Redpanda Avro consumer that consumes the last N messages from a topic.
 * Configuration is loaded from consumer.properties file.
 * Exposes Prometheus metrics on /metrics endpoint.
 */
public class AvroConsumer {
    private static Properties config;
    
    // Prometheus Metrics
    private static final Counter messagesConsumedTotal = Counter.build()
        .name("kafka_consumer_messages_consumed_total")
        .help("Total number of messages consumed")
        .labelNames("topic", "partition")
        .register();
    
    private static final Histogram messageProcessingTime = Histogram.build()
        .name("kafka_consumer_message_processing_seconds")
        .help("Time taken to process a message")
        .labelNames("topic", "partition")
        .register();
    
    private static final Gauge messagesConsumedCurrent = Gauge.build()
        .name("kafka_consumer_messages_consumed_current")
        .help("Current number of messages consumed in this session")
        .labelNames("topic")
        .register();
    
    private static final Counter consumerErrorsTotal = Counter.build()
        .name("kafka_consumer_errors_total")
        .help("Total number of consumer errors")
        .labelNames("error_type")
        .register();
    
    private static HTTPServer metricsServer;
    
    /**
     * Loads configuration from consumer.properties file.
     */
    private static Properties loadConfiguration() {
        Properties props = new Properties();
        try {
            InputStream inputStream = AvroConsumer.class.getClassLoader()
                .getResourceAsStream("consumer.properties");
            
            if (inputStream == null) {
                throw new RuntimeException("Configuration file 'consumer.properties' not found in classpath");
            }
            
            props.load(inputStream);
            inputStream.close();
            
            System.out.println("Configuration loaded from consumer.properties");
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration from consumer.properties: " + e.getMessage(), e);
        }
        return props;
    }
    
    public static void main(String[] args) {
        // Load configuration from properties file
        config = loadConfiguration();
        
        // Get number of messages to consume from command line argument (optional)
        int numMessages = Integer.MAX_VALUE; // default: consume all messages
        if (args.length > 0) {
            try {
                numMessages = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid number of messages: " + args[0] + ". Will consume all messages from offset 0");
            }
        }
        
        // Get configuration values
        String bootstrapServers = config.getProperty("bootstrap.servers");
        String topicName = config.getProperty("topic.name");
        String schemaRegistryUrl = config.getProperty("schema.registry.url");
        String securityProtocol = config.getProperty("security.protocol");
        String saslMechanism = config.getProperty("sasl.mechanism");
        
        System.out.println("==========================================");
        System.out.println("Redpanda Avro Consumer");
        System.out.println("==========================================");
        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Topic: " + topicName);
        System.out.println("Schema Registry URL: " + schemaRegistryUrl);
        System.out.println("Security protocol: " + securityProtocol);
        System.out.println("SASL mechanism: " + saslMechanism);
        System.out.println("Number of messages to consume: " + (numMessages == Integer.MAX_VALUE ? "all (from offset 0)" : numMessages));
        System.out.println("==========================================\n");
        
        // Start Prometheus metrics HTTP server
        int metricsPort = Integer.parseInt(config.getProperty("metrics.port", "8080"));
        try {
            metricsServer = new HTTPServer(metricsPort);
            System.out.println("Prometheus metrics server started on port " + metricsPort);
            System.out.println("Metrics endpoint: http://localhost:" + metricsPort + "/metrics");
        } catch (IOException e) {
            System.err.println("Failed to start metrics server on port " + metricsPort + ": " + e.getMessage());
            System.err.println("Continuing without metrics server...");
        }
        
        KafkaConsumer<String, Object> consumer = null;
        
        try {
            // Create consumer properties
            Properties props = createConsumerProperties();
            
            System.out.println("Creating KafkaConsumer with Avro deserializer...");
            consumer = new KafkaConsumer<>(props);
            
            System.out.println("Subscribing to topic: " + topicName);
            consumer.subscribe(Collections.singletonList(topicName));
            
            // Wait for partition assignment with retry logic
            System.out.println("Waiting for partition assignment...");
            List<TopicPartition> partitions = Collections.emptyList();
            int maxRetries = 10;
            int retryCount = 0;
            
            while (partitions.isEmpty() && retryCount < maxRetries) {
                System.out.println("Poll attempt " + (retryCount + 1) + "/" + maxRetries + "...");
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(5));
                
                partitions = consumer.assignment().stream()
                    .collect(Collectors.toList());
                
                if (partitions.isEmpty()) {
                    retryCount++;
                    if (retryCount < maxRetries) {
                        System.out.println("No partitions assigned yet, retrying...");
                        try {
                            Thread.sleep(1000); // Wait 1 second before retry
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
            
            if (partitions.isEmpty()) {
                System.out.println("==========================================");
                System.out.println("ERROR: No partitions assigned after " + maxRetries + " attempts");
                System.out.println("==========================================");
                System.out.println("Possible causes:");
                System.out.println("  1. Topic '" + topicName + "' does not exist");
                System.out.println("  2. Cannot connect to cluster (check bootstrap servers)");
                System.out.println("  3. Authentication/authorization failure");
                System.out.println("  4. Consumer group coordinator unavailable");
                System.out.println("\nTroubleshooting:");
                System.out.println("  - Verify topic exists: Check if topic '" + topicName + "' exists in the cluster");
                System.out.println("  - Check connection: Verify bootstrap servers are reachable");
                System.out.println("  - Check credentials: Verify username/password are correct");
                System.out.println("  - Check permissions: Verify user has READ permission for the topic");
                System.out.println("==========================================");
                return;
            }
            
            System.out.println("Successfully assigned partitions: " + partitions);
            
            // Seek to offset 0 for all partitions
            System.out.println("Seeking to offset 0 for all partitions...");
            for (TopicPartition partition : partitions) {
                consumer.seek(partition, 0);
                System.out.println("Partition " + partition.partition() + ": seeking to offset 0");
            }
            
            System.out.println("\nConsuming messages from offset 0...\n");
            
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
                    
                    // Start timing for metrics
                    Histogram.Timer timer = messageProcessingTime
                        .labels(topicName, String.valueOf(record.partition()))
                        .startTimer();
                    
                    try {
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
                        
                        // Update Prometheus metrics
                        messagesConsumedTotal.labels(topicName, String.valueOf(record.partition())).inc();
                        messagesConsumedCurrent.labels(topicName).inc();
                        
                        messageCount++;
                    } catch (Exception e) {
                        // Track errors in metrics
                        consumerErrorsTotal.labels(e.getClass().getSimpleName()).inc();
                        System.err.println("Error processing message: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        // Record processing time
                        timer.observeDuration();
                    }
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
            
            // Stop metrics server
            if (metricsServer != null) {
                try {
                    System.out.println("Stopping metrics server...");
                    metricsServer.stop();
                    System.out.println("Metrics server stopped.");
                } catch (Exception e) {
                    System.err.println("Warning: Error stopping metrics server: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Creates properties for Kafka Consumer with Avro deserializer and SASL_SSL authentication.
     * Properties are loaded from the configuration file.
     */
    private static Properties createConsumerProperties() {
        Properties props = new Properties();
        
        // Bootstrap servers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("bootstrap.servers"));
        
        // Consumer group ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty("consumer.group.id", "avro-consumer-group"));
        
        // Key deserializer (String)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // Value deserializer (Avro)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        
        // Schema Registry URL
        props.put("schema.registry.url", config.getProperty("schema.registry.url"));
        
        // Auto offset reset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
            config.getProperty("auto.offset.reset", "earliest"));
        
        // Session timeout
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 
            config.getProperty("session.timeout.ms", "30000"));
        
        // Heartbeat interval
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 
            config.getProperty("heartbeat.interval.ms", "10000"));
        
        // Request timeout
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 
            config.getProperty("request.timeout.ms", "30000"));
        
        // Metadata fetch timeout
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 
            config.getProperty("max.poll.interval.ms", "300000"));
        
        // SSL/Trust Store configuration
        String truststorePath = config.getProperty("ssl.truststore.path", "").trim();
        if (!truststorePath.isEmpty()) {
            // Trust store location
            props.put("ssl.truststore.location", truststorePath);
            
            // Trust store password
            String truststorePassword = config.getProperty("ssl.truststore.password", "").trim();
            if (!truststorePassword.isEmpty()) {
                props.put("ssl.truststore.password", truststorePassword);
            }
        }
        
        // SASL configuration
        props.put("security.protocol", config.getProperty("security.protocol", "SASL_SSL"));
        props.put("sasl.mechanism", config.getProperty("sasl.mechanism", "SCRAM-SHA-512"));
        
        // Build SASL JAAS config from username and password
        String username = config.getProperty("sasl.username");
        String password = config.getProperty("sasl.password");
        String saslJaasConfig = String.format(
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
            username, password);
        props.put("sasl.jaas.config", saslJaasConfig);
        
        return props;
    }
}

