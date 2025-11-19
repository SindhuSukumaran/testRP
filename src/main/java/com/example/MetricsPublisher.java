package com.example;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.util.Random;

/**
 * Simple Prometheus metrics publisher that exposes dummy metrics.
 * Runs an HTTP server on port 8080 (configurable) to expose /metrics endpoint.
 */
public class MetricsPublisher {
    // Metrics port - change this if needed
    private static final int METRICS_PORT = 8083;
    
    // Prometheus Metrics
    private static final Counter requestCounter = Counter.build()
        .name("http_requests_total")
        .help("Total number of HTTP requests")
        .labelNames("method", "status")
        .register();
    
    private static final Gauge activeConnections = Gauge.build()
        .name("active_connections")
        .help("Number of active connections")
        .register();
    
    private static final Histogram requestDuration = Histogram.build()
        .name("http_request_duration_seconds")
        .help("HTTP request duration in seconds")
        .labelNames("method")
        .register();
    
    private static final Counter errorCounter = Counter.build()
        .name("errors_total")
        .help("Total number of errors")
        .labelNames("error_type")
        .register();
    
    private static final Gauge memoryUsage = Gauge.build()
        .name("memory_usage_bytes")
        .help("Memory usage in bytes")
        .register();
    
    private static final Counter messagesProcessed = Counter.build()
        .name("messages_processed_total")
        .help("Total number of messages processed")
        .register();
    
    private static HTTPServer metricsServer;
    private static volatile boolean running = true;
    
    public static void main(String[] args) {
        int port = METRICS_PORT;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[0] + ". Using default: " + METRICS_PORT);
            }
        }
        
        System.out.println("==========================================");
        System.out.println("Prometheus Metrics Publisher");
        System.out.println("==========================================");
        System.out.println("Starting metrics HTTP server on port: " + port);
        System.out.println("Metrics endpoint: http://localhost:" + port + "/metrics");
        System.out.println("==========================================\n");
        
        try {
            // Start Prometheus metrics HTTP server
            metricsServer = new HTTPServer(port);
            System.out.println("✓ Metrics server started successfully!");
            System.out.println("✓ Prometheus can scrape metrics from: http://host.docker.internal:" + port + "/metrics");
            System.out.println("  (or http://localhost:" + port + "/metrics from your machine)\n");
            
            // Start a background thread to update metrics periodically
            Thread metricsUpdater = new Thread(() -> {
                Random random = new Random();
                while (running) {
                    try {
                        // Simulate some activity and update metrics
                        updateMetrics(random);
                        Thread.sleep(5000); // Update every 5 seconds
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });
            metricsUpdater.setDaemon(true);
            metricsUpdater.start();
            
            System.out.println("Metrics are being updated every 5 seconds...");
            System.out.println("Press Ctrl+C to stop the server.\n");
            
            // Keep the main thread alive
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down metrics server...");
                running = false;
                if (metricsServer != null) {
                    metricsServer.stop();
                }
                System.out.println("Metrics server stopped.");
            }));
            
            // Wait indefinitely
            Thread.sleep(Long.MAX_VALUE);
            
        } catch (IOException e) {
            System.err.println("Failed to start metrics server on port " + port + ": " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            System.out.println("Interrupted. Shutting down...");
            Thread.currentThread().interrupt();
        } finally {
            if (metricsServer != null) {
                metricsServer.stop();
            }
        }
    }
    
    /**
     * Updates metrics with dummy values to simulate application activity.
     */
    private static void updateMetrics(Random random) {
        // Simulate HTTP requests
        String[] methods = {"GET", "POST", "PUT", "DELETE"};
        String[] statuses = {"200", "404", "500"};
        
        for (int i = 0; i < random.nextInt(5) + 1; i++) {
            String method = methods[random.nextInt(methods.length)];
            String status = statuses[random.nextInt(statuses.length)];
            requestCounter.labels(method, status).inc();
            
            // Record request duration
            Histogram.Timer timer = requestDuration.labels(method).startTimer();
            try {
                Thread.sleep(random.nextInt(100)); // Simulate processing time
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            timer.observeDuration();
        }
        
        // Update active connections (random between 10 and 100)
        activeConnections.set(random.nextInt(90) + 10);
        
        // Simulate some errors
        if (random.nextDouble() < 0.1) { // 10% chance of error
            String[] errorTypes = {"timeout", "connection_error", "validation_error"};
            String errorType = errorTypes[random.nextInt(errorTypes.length)];
            errorCounter.labels(errorType).inc();
        }
        
        // Update memory usage (simulate between 50MB and 500MB)
        long memoryBytes = (random.nextInt(450) + 50) * 1024 * 1024;
        memoryUsage.set(memoryBytes);
        
        // Simulate messages processed
        int messages = random.nextInt(10) + 1;
        messagesProcessed.inc(messages);
        
        System.out.println("Metrics updated - Active connections: " + (int)activeConnections.get() + 
                          ", Memory: " + (memoryBytes / 1024 / 1024) + "MB, Messages: " + messages);
    }
}

