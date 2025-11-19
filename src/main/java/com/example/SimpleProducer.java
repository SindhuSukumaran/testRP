package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * Simple producer class to test connection to Redpanda cluster and produce messages.
 * Produces messages at random intervals (1s, 3s, 5s, 10s).
 * Handles all possible exceptions and prints detailed stack traces on failure.
 */
public class SimpleProducer {
    // Connection details
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String DEFAULT_TOPIC = "quickstart";
    
    // Random interval options (in seconds)
    private static final List<Integer> INTERVAL_OPTIONS = Arrays.asList(1, 3, 5, 10);
    
    private static final Random random = new Random();
    
    public static void main(String[] args) {
        // Get topic name from command line argument or use default
        String topicName = (args.length > 0 && args[0] != null && !args[0].isEmpty()) 
            ? args[0] 
            : DEFAULT_TOPIC;
        
        System.out.println("==========================================");
        System.out.println("Simple Producer Connection Test");
        System.out.println("==========================================");
        System.out.println("Bootstrap servers: " + BOOTSTRAP_SERVERS);
        System.out.println("Security protocol: PLAINTEXT (no authentication)");
        System.out.println("Topic: " + topicName);
        System.out.println("Random intervals: " + INTERVAL_OPTIONS + " seconds");
        System.out.println("==========================================\n");
        
        KafkaProducer<String, String> producer = null;
        
        try {
            // Create producer properties
            Properties props = createProducerProperties();
            
            System.out.println("Creating KafkaProducer instance...");
            producer = new KafkaProducer<>(props);
            
            System.out.println("Attempting to connect to cluster...");
            System.out.println("(This may take a few seconds to establish connection)...\n");
            
            // Test connection by sending a test message (synchronous to verify connection)
            // Note: The topic must exist before sending messages
            System.out.println("Testing connection by sending a test message...");
            System.out.println("Note: Topic '" + topicName + "' must exist in the cluster.");
            System.out.println("If the topic doesn't exist, create it first or use an existing topic.\n");
            
            ProducerRecord<String, String> testRecord = new ProducerRecord<>(topicName, "test-key", "Connection test message");
            Future<RecordMetadata> future = producer.send(testRecord);
            
            try {
                RecordMetadata metadata = future.get(); // Wait for send to complete
                
                // If we get here, the connection was successful
                System.out.println("==========================================");
                System.out.println("SUCCESS: Producer connection established!");
                System.out.println("==========================================");
                System.out.println("Producer successfully connected to Redpanda cluster.");
                System.out.println("Test message sent to topic: " + topicName);
                System.out.println("Test message partition: " + metadata.partition());
                System.out.println("Test message offset: " + metadata.offset());
                System.out.println("Connection test completed successfully.\n");
            } catch (java.util.concurrent.ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof org.apache.kafka.common.errors.TimeoutException) {
                    System.out.println("==========================================");
                    System.out.println("ERROR: Topic not found or connection timeout");
                    System.out.println("==========================================");
                    System.out.println("Error: Topic '" + topicName + "' not present in metadata after timeout.");
                    System.out.println("\nPossible causes:");
                    System.out.println("  1. Topic '" + topicName + "' does not exist in the cluster");
                    System.out.println("  2. Cannot connect to cluster (network issues)");
                    System.out.println("  3. Topic metadata fetch timed out");
                    System.out.println("\nSolutions:");
                    System.out.println("  - Create the topic first using:");
                    System.out.println("    rpk topic create " + topicName);
                    System.out.println("  - Or use an existing topic name");
                    System.out.println("  - Or check if the cluster is accessible");
                    System.out.println("==========================================");
                    throw new RuntimeException("Topic '" + topicName + "' not found. Please create it first.", cause);
                } else {
                    throw e;
                }
            }
            
            // Start producing messages at random intervals
            System.out.println("==========================================");
            System.out.println("Starting message production...");
            System.out.println("==========================================");
            System.out.println("Producing messages at random intervals (press Ctrl+C to stop).");
            System.out.println("==========================================\n");
            
            int messageCount = 0;
            boolean shouldContinue = true;
            
            while (shouldContinue) {
                try {
                    // Select random interval from options
                    int intervalSeconds = INTERVAL_OPTIONS.get(random.nextInt(INTERVAL_OPTIONS.size()));
                    
                    // Create message
                    messageCount++;
                    String messageKey = "key-" + messageCount;
                    String messageValue = "Message #" + messageCount + " at " + System.currentTimeMillis();
                    
                    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, messageKey, messageValue);
                    
                    // Send message asynchronously
                    producer.send(record, (recordMetadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Error sending message: " + exception.getMessage());
                            exception.printStackTrace();
                        } else {
                            System.out.println("----------------------------------------");
                            System.out.println("Message sent successfully");
                            System.out.println("Topic: " + recordMetadata.topic());
                            System.out.println("Partition: " + recordMetadata.partition());
                            System.out.println("Offset: " + recordMetadata.offset());
                            System.out.println("Timestamp: " + recordMetadata.timestamp());
                            System.out.println("Key: " + messageKey);
                            System.out.println("Value: " + messageValue);
                            System.out.println("----------------------------------------\n");
                        }
                    });
                    
                    // Wait for the random interval before sending next message
                    System.out.println("Waiting " + intervalSeconds + " second(s) before next message...");
                    Thread.sleep(intervalSeconds * 1000L);
                    
                } catch (InterruptedException e) {
                    System.out.println("Interrupted. Stopping message production.");
                    shouldContinue = false;
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.out.println("Error while producing message: " + e.getMessage());
                    e.printStackTrace();
                    // Continue producing despite errors, unless it's a critical error
                    if (e instanceof InterruptedException) {
                        shouldContinue = false;
                        Thread.currentThread().interrupt();
                    }
                }
            }
            
            System.out.println("==========================================");
            System.out.println("Message production completed.");
            System.out.println("Total messages sent: " + messageCount);
            System.out.println("==========================================");
            
        } catch (AuthenticationException e) {
            System.out.println("==========================================");
            System.out.println("FAILED: Authentication Exception");
            System.out.println("==========================================");
            System.out.println("Error: Failed to authenticate with the cluster.");
            System.out.println("Possible causes:");
            System.out.println("  - Incorrect username or password");
            System.out.println("  - Wrong SASL mechanism");
            System.out.println("  - User does not exist in the cluster");
            System.out.println("  - Authentication service unavailable");
            System.out.println("\nException Details:");
            System.out.println("Exception Type: " + e.getClass().getName());
            System.out.println("Message: " + e.getMessage());
            System.out.println("\nFull Stack Trace:");
            e.printStackTrace();
            System.exit(1);
            
        } catch (AuthorizationException e) {
            System.out.println("==========================================");
            System.out.println("FAILED: Authorization Exception");
            System.out.println("==========================================");
            System.out.println("Error: User is authenticated but lacks required permissions.");
            System.out.println("Possible causes:");
            System.out.println("  - User does not have WRITE permission for the topic");
            System.out.println("  - Topic access is restricted");
            System.out.println("\nException Details:");
            System.out.println("Exception Type: " + e.getClass().getName());
            System.out.println("Message: " + e.getMessage());
            System.out.println("\nFull Stack Trace:");
            e.printStackTrace();
            System.exit(1);
            
        } catch (TimeoutException e) {
            System.out.println("==========================================");
            System.out.println("FAILED: Timeout Exception");
            System.out.println("==========================================");
            System.out.println("Error: Connection or operation timed out.");
            System.out.println("Possible causes:");
            System.out.println("  - Cluster is unreachable (network issues)");
            System.out.println("  - Bootstrap servers are incorrect");
            System.out.println("  - Firewall blocking connections");
            System.out.println("  - Cluster is down or overloaded");
            System.out.println("  - Connection timeout too short");
            System.out.println("\nException Details:");
            System.out.println("Exception Type: " + e.getClass().getName());
            System.out.println("Message: " + e.getMessage());
            System.out.println("\nFull Stack Trace:");
            e.printStackTrace();
            System.exit(1);
            
        } catch (KafkaException e) {
            System.out.println("==========================================");
            System.out.println("FAILED: Kafka Exception");
            System.out.println("==========================================");
            System.out.println("Error: A Kafka-specific error occurred.");
            System.out.println("Possible causes:");
            System.out.println("  - Invalid configuration");
            System.out.println("  - Cluster metadata error");
            System.out.println("  - Protocol version mismatch");
            System.out.println("  - Serialization/deserialization error");
            System.out.println("\nException Details:");
            System.out.println("Exception Type: " + e.getClass().getName());
            System.out.println("Message: " + e.getMessage());
            System.out.println("\nFull Stack Trace:");
            e.printStackTrace();
            System.exit(1);
            
        } catch (IllegalArgumentException e) {
            System.out.println("==========================================");
            System.out.println("FAILED: Configuration Exception");
            System.out.println("==========================================");
            System.out.println("Error: Invalid configuration parameter.");
            System.out.println("Possible causes:");
            System.out.println("  - Invalid bootstrap servers format");
            System.out.println("  - Missing required configuration");
            System.out.println("  - Invalid property value");
            System.out.println("\nException Details:");
            System.out.println("Exception Type: " + e.getClass().getName());
            System.out.println("Message: " + e.getMessage());
            System.out.println("\nFull Stack Trace:");
            e.printStackTrace();
            System.exit(1);
            
        } catch (IllegalStateException e) {
            System.out.println("==========================================");
            System.out.println("FAILED: Illegal State Exception");
            System.out.println("==========================================");
            System.out.println("Error: Producer is in an invalid state.");
            System.out.println("Possible causes:");
            System.out.println("  - Producer was closed");
            System.out.println("  - Producer not properly initialized");
            System.out.println("  - Concurrent access violation");
            System.out.println("\nException Details:");
            System.out.println("Exception Type: " + e.getClass().getName());
            System.out.println("Message: " + e.getMessage());
            System.out.println("\nFull Stack Trace:");
            e.printStackTrace();
            System.exit(1);
            
        } catch (Exception e) {
            System.out.println("==========================================");
            System.out.println("FAILED: Unexpected Exception");
            System.out.println("==========================================");
            System.out.println("Error: An unexpected error occurred.");
            System.out.println("Possible causes:");
            System.out.println("  - Network connectivity issues");
            System.out.println("  - JVM/System errors");
            System.out.println("  - Unknown configuration issues");
            System.out.println("\nException Details:");
            System.out.println("Exception Type: " + e.getClass().getName());
            System.out.println("Message: " + e.getMessage());
            System.out.println("\nFull Stack Trace:");
            e.printStackTrace();
            System.exit(1);
            
        } finally {
            if (producer != null) {
                try {
                    System.out.println("\nClosing producer...");
                    producer.close();
                    System.out.println("Producer closed successfully.");
                } catch (Exception e) {
                    System.out.println("Warning: Error while closing producer:");
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * Creates properties for Kafka Producer (no authentication - PLAINTEXT).
     */
    private static Properties createProducerProperties() {
        Properties props = new Properties();
        
        // Bootstrap servers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        
        // Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Acknowledgment - wait for all replicas
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        
        // Retries
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        
        // Batch size
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        
        // Linger time
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        
        // Buffer memory
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        
        // Request timeout
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        
        // Metadata fetch timeout - how long to wait for topic metadata
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000); // 60 seconds
        
        // No authentication - using PLAINTEXT
        // Security protocol is PLAINTEXT (no SSL, no SASL)
        props.put("security.protocol", "PLAINTEXT");
        
        return props;
    }
}

