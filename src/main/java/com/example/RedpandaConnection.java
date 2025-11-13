package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Simple class to connect to Redpanda cluster and verify the connection.
 */
public class RedpandaConnection {
    // Connection details from rpk-profile.yaml
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092";
    private static final String SASL_USERNAME = "superuser";
    private static final String SASL_PASSWORD = "secretpassword";
    private static final String SASL_MECHANISM = "SCRAM-SHA-256";
    
    public static void main(String[] args) {
        System.out.println("Starting Redpanda connection test...");
        
        try {
            // Create admin client to check cluster connection
            Properties adminProps = createAdminProperties();
            AdminClient adminClient = AdminClient.create(adminProps);
            
            System.out.println("Attempting to connect to Redpanda cluster at: " + BOOTSTRAP_SERVERS);
            
            // List topics to verify connection
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topics = topicsResult.names().get(5, TimeUnit.SECONDS);
            
            System.out.println("Successfully connected to Redpanda cluster!");
            System.out.println("Found " + topics.size() + " topic(s) in the cluster");
            
            if (topics.isEmpty()) {
                System.out.println("No topics found in the cluster (this is normal for a new cluster)");
            } else {
                System.out.println("Topics: " + topics);
            }
            
            // Get cluster description
            adminClient.describeCluster().nodes().get(5, TimeUnit.SECONDS).forEach(node -> {
                System.out.println("Cluster node: " + node.host() + ":" + node.port());
            });
            
            adminClient.close();
            System.out.println("Connection test completed successfully!");
            
        } catch (Exception e) {
            System.out.println("Failed to connect to Redpanda cluster: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Creates properties for AdminClient with SASL authentication.
     */
    private static Properties createAdminProperties() {
        Properties props = new Properties();
        
        // Bootstrap servers
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        
        // SASL configuration
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", SASL_MECHANISM);
        props.put("sasl.jaas.config", 
            String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                SASL_USERNAME, SASL_PASSWORD));
        
        // Connection timeout
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        
        return props;
    }
    
    /**
     * Creates properties for Kafka Producer (for future use).
     */
    public static Properties createProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // SASL configuration
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", SASL_MECHANISM);
        props.put("sasl.jaas.config", 
            String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                SASL_USERNAME, SASL_PASSWORD));
        
        return props;
    }
    
    /**
     * Creates properties for Kafka Consumer (for future use).
     */
    public static Properties createConsumerProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // SASL configuration
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", SASL_MECHANISM);
        props.put("sasl.jaas.config", 
            String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                SASL_USERNAME, SASL_PASSWORD));
        
        return props;
    }
}

