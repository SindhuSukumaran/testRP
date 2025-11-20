package com.example;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.avro.generic.GenericRecord;
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
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
    
    // EventType metrics
    private static final Counter eventTypeCount = Counter.build()
        .name("kafka_consumer_eventtype_total")
        .help("Total count of messages by EVENTTYPE_ value")
        .labelNames("topic", "partition", "eventtype")
        .register();
    
    private static final Gauge eventTypeByTimestamp = Gauge.build()
        .name("kafka_consumer_eventtype_by_timestamp")
        .help("Kafka timestamp mapped to EVENTTYPE_ value")
        .labelNames("topic", "partition", "eventtype", "kafka_timestamp")
        .register();
    
    private static final Gauge uniqueEventTypeCount = Gauge.build()
        .name("kafka_consumer_unique_eventtype_count")
        .help("Number of unique EVENTTYPE_ values seen")
        .labelNames("topic")
        .register();
    
    private static HTTPServer metricsServer;
    
    // Track unique EVENTTYPE_ values per topic
    private static final Set<String> uniqueEventTypes = new HashSet<>();
    
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
        System.out.println("Consume mode: All messages from offset 0, then continue for new messages");
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
            
            // Important: After partition assignment, poll once to ensure partition assignment is complete
            consumer.poll(Duration.ofMillis(100));
            
            // Seek to offset 0 for all partitions to read all messages from the beginning
            System.out.println("Seeking to offset 0 for all partitions...");
            for (TopicPartition partition : partitions) {
                consumer.seek(partition, 0);
                System.out.println("Partition " + partition.partition() + ": seeking to offset 0");
            }
            
            boolean initialConsumptionComplete = false;
            boolean continuousMode = false;
            
            System.out.println("\nConsuming all messages from offset 0...");
            System.out.println("After consuming all existing messages, will continue for new messages...");
            System.out.println("Press Ctrl+C to stop.\n");
            
            // Consume messages
            int messageCount = 0;
            long endTime = Long.MAX_VALUE; // No timeout - run continuously
            
            while (System.currentTimeMillis() < endTime) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(5));
                
                if (records.isEmpty()) {
                    // If we get empty records and haven't switched to continuous mode yet,
                    // it means we've consumed all existing messages, so switch to continuous mode
                    if (!initialConsumptionComplete) {
                        System.out.println("\n==========================================");
                        System.out.println("Successfully consumed all existing messages: " + messageCount + " message(s)");
                        System.out.println("Now continuing to consume new messages...");
                        System.out.println("Waiting for new messages...");
                        System.out.println("==========================================\n");
                        initialConsumptionComplete = true;
                        continuousMode = true;
                    } else {
                        // In continuous mode, empty records just means no new messages yet
                        continue;
                    }
                } else {
                    // If we get records and haven't switched to continuous mode yet,
                    // we're still consuming initial messages
                    if (!initialConsumptionComplete && records.isEmpty()) {
                        // This shouldn't happen, but just in case
                        continue;
                    }
                }
                
                for (ConsumerRecord<String, Object> record : records) {
                    
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
                        
                        // Extract EVENTTYPE_ from Avro value
                        String eventType = extractEventType(record.value());
                        if (eventType != null) {
                            System.out.println("EVENTTYPE_: " + eventType);
                            
                            // Track unique EVENTTYPE_ values
                            synchronized (uniqueEventTypes) {
                                uniqueEventTypes.add(eventType);
                                uniqueEventTypeCount.labels(topicName).set(uniqueEventTypes.size());
                            }
                            
                            // Count messages by EVENTTYPE_
                            eventTypeCount.labels(topicName, String.valueOf(record.partition()), eventType).inc();
                            
                            // Map Kafka timestamp to EVENTTYPE_
                            eventTypeByTimestamp.labels(topicName, String.valueOf(record.partition()), 
                                eventType, String.valueOf(record.timestamp())).set(1.0);
                        } else {
                            System.out.println("EVENTTYPE_: not found or null");
                        }
                        
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
            
            // Note: Metrics server is kept running for continuous modes
            // It will only stop when the JVM exits
            // For continuous modes (latest, or last after initial consumption), 
            // the metrics server should remain up
        }
    }
    
    /**
     * Extracts EVENTTYPE_ field from Avro GenericRecord.
     * @param value The Avro value (should be a GenericRecord)
     * @return The EVENTTYPE_ value as String, or null if not found
     */
    private static String extractEventType(Object value) {
        if (value == null) {
            return null;
        }
        
        try {
            if (value instanceof GenericRecord) {
                GenericRecord record = (GenericRecord) value;
                Object eventTypeObj = record.get("EVENTTYPE_");
                if (eventTypeObj != null) {
                    return eventTypeObj.toString();
                }
            }
        } catch (Exception e) {
            System.err.println("Error extracting EVENTTYPE_: " + e.getMessage());
        }
        
        return null;
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
        
        // Offset commit configuration
        // Disable auto-commit to prevent offset commits when manually seeking
        // This ensures each run starts from the seek position, not from last committed offset
        String enableAutoCommit = config.getProperty("enable.auto.commit", "false");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.parseBoolean(enableAutoCommit));
        
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

