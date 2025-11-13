package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Simple consumer class to test connection to Redpanda cluster.
 * Handles all possible exceptions and prints detailed stack traces on failure.
 */
public class SimpleConsumer {
    // Connection details
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092";
    private static final String SASL_USERNAME = "superuser";
    private static final String SASL_PASSWORD = "secretpassword";
    private static final String SASL_MECHANISM = "SCRAM-SHA-512";
    private static final String DEFAULT_TOPIC = "test-topic";
    private static final String DEFAULT_GROUP_ID = "simple-consumer-group";
    
    // SSL/Trust Store configuration
    // Set these to null if not using SSL/TLS
    private static final String SSL_TRUSTSTORE_LOCATION = null; // e.g., "/path/to/truststore.jks"
    private static final String SSL_TRUSTSTORE_PASSWORD = null; // e.g., "truststore-password"
    
    public static void main(String[] args) {
        // Get topic name from command line argument or use default
        String topicName = (args.length > 0 && args[0] != null && !args[0].isEmpty()) 
            ? args[0] 
            : DEFAULT_TOPIC;
        
        System.out.println("==========================================");
        System.out.println("Simple Consumer Connection Test");
        System.out.println("==========================================");
        System.out.println("Bootstrap servers: " + BOOTSTRAP_SERVERS);
        System.out.println("Security protocol: SASL_SSL");
        System.out.println("SASL mechanism: " + SASL_MECHANISM);
        System.out.println("SASL username: " + SASL_USERNAME);
        System.out.println("SASL password: ***");
        System.out.println("SASL JAAS config: org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + SASL_USERNAME + "\" password=\"***\";");
        System.out.println("Topic: " + topicName);
        System.out.println("Consumer group: " + DEFAULT_GROUP_ID);
        if (SSL_TRUSTSTORE_LOCATION != null && !SSL_TRUSTSTORE_LOCATION.isEmpty()) {
            System.out.println("Trust store location: " + SSL_TRUSTSTORE_LOCATION);
            System.out.println("Trust store password: " + (SSL_TRUSTSTORE_PASSWORD != null ? "***" : "not set"));
        } else {
            System.out.println("Warning: Trust store location not set (required for SASL_SSL)");
        }
        System.out.println("==========================================\n");
        
        KafkaConsumer<String, String> consumer = null;
        
        try {
            // Create consumer properties with SASL authentication
            Properties props = createConsumerProperties();
            
            System.out.println("Creating KafkaConsumer instance...");
            consumer = new KafkaConsumer<>(props);
            
            System.out.println("Subscribing to topic: " + topicName);
            consumer.subscribe(Collections.singletonList(topicName));
            
            System.out.println("Attempting to connect to cluster and fetch metadata...");
            System.out.println("(This may take a few seconds to establish connection)...\n");
            
            // Attempt to poll to establish connection and verify we can communicate with the cluster
            // This will trigger connection, authentication, and metadata fetch
            consumer.poll(Duration.ofSeconds(5));
            
            // If we get here, the connection was successful
            System.out.println("==========================================");
            System.out.println("SUCCESS: Consumer connection established!");
            System.out.println("==========================================");
            System.out.println("Consumer successfully connected to Redpanda cluster.");
            System.out.println("Consumer is subscribed to topic: " + topicName);
            System.out.println("Connection test completed successfully.\n");
            
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
            System.out.println("  - User does not have READ permission for the topic");
            System.out.println("  - User does not have permission to join consumer group");
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
            System.out.println("Error: Consumer is in an invalid state.");
            System.out.println("Possible causes:");
            System.out.println("  - Consumer was closed");
            System.out.println("  - Consumer not properly initialized");
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
     * Creates properties for Kafka Consumer with SASL authentication.
     */
    private static Properties createConsumerProperties() {
        Properties props = new Properties();
        
        // Bootstrap servers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        
        // Consumer group ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, DEFAULT_GROUP_ID);
        
        // Deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // Auto offset reset - start from earliest if no offset exists
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
        // Trust store is required for SASL_SSL
        if (SSL_TRUSTSTORE_LOCATION != null && !SSL_TRUSTSTORE_LOCATION.isEmpty()) {
            // Trust store location
            props.put("ssl.truststore.location", SSL_TRUSTSTORE_LOCATION);
            
            // Trust store password
            if (SSL_TRUSTSTORE_PASSWORD != null && !SSL_TRUSTSTORE_PASSWORD.isEmpty()) {
                props.put("ssl.truststore.password", SSL_TRUSTSTORE_PASSWORD);
            }
        }
        
        // SASL configuration
        // Security protocol is SASL_SSL
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", SASL_MECHANISM);
        props.put("sasl.jaas.config", 
            String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                SASL_USERNAME, SASL_PASSWORD));
        
        return props;
    }
}

