package com.example;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * API Test Runner that mimics running 100 sequential API test cases.
 * Publishes Prometheus metrics for test execution with dimensions:
 * - Date of execution
 * - Test case ID
 * - Error code (HTTP status)
 * - Latency
 * - Success/Failure
 */
public class ApiTestRunner {
    // Metrics port
    private static final int METRICS_PORT = 8084;
    
    // Test configuration
    private static final int TOTAL_TEST_CASES = 100;
    private static final double MIN_LATENCY_SECONDS = 0.5;
    private static final double MAX_LATENCY_SECONDS = 3.0;
    private static final double FAILURE_RATE = 0.15; // 15% of tests will fail
    
    // API definitions
    private static final List<String> API_NAMES = Arrays.asList(
        "user-service", "order-service", "payment-service", "inventory-service",
        "auth-service", "notification-service", "analytics-service", "search-service",
        "report-service", "config-service"
    );
    
    // HTTP status codes for failures
    private static final List<Integer> ERROR_CODES = Arrays.asList(400, 401, 403, 404, 500, 502, 503);
    
    // Prometheus Metrics
    private static final Counter testExecutionCounter = Counter.build()
        .name("api_test_executions_total")
        .help("Total number of API test executions")
        .labelNames("test_case_id", "api_name", "status_code", "success", "execution_date")
        .register();
    
    private static final Histogram testLatency = Histogram.build()
        .name("api_test_latency_seconds")
        .help("API test execution latency in seconds")
        .labelNames("test_case_id", "api_name", "status_code", "execution_date")
        .buckets(0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 5.0) // Latency buckets
        .register();
    
    private static final Gauge testLatencyGauge = Gauge.build()
        .name("api_test_latency_seconds_gauge")
        .help("API test execution latency in seconds (plain gauge metric)")
        .labelNames("test_case_id", "api_name", "status_code", "execution_date")
        .register();
    
    private static final Counter testSuccessCounter = Counter.build()
        .name("api_test_success_total")
        .help("Total number of successful API tests")
        .labelNames("test_case_id", "api_name", "execution_date")
        .register();
    
    private static final Counter testFailureCounter = Counter.build()
        .name("api_test_failure_total")
        .help("Total number of failed API tests")
        .labelNames("test_case_id", "api_name", "status_code", "execution_date")
        .register();
    
    private static HTTPServer metricsServer;
    private static final Random random = new Random();
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
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
        System.out.println("API Test Runner");
        System.out.println("==========================================");
        System.out.println("Total test cases: " + TOTAL_TEST_CASES);
        System.out.println("APIs to test: " + API_NAMES.size());
        System.out.println("Failure rate: " + (FAILURE_RATE * 100) + "%");
        System.out.println("Latency range: " + MIN_LATENCY_SECONDS + "s - " + MAX_LATENCY_SECONDS + "s");
        System.out.println("Metrics port: " + port);
        System.out.println("==========================================\n");
        
        try {
            // Start Prometheus metrics HTTP server
            metricsServer = new HTTPServer(port);
            System.out.println("✓ Metrics server started on port " + port);
            System.out.println("✓ Metrics endpoint: http://localhost:" + port + "/metrics");
            System.out.println("✓ Prometheus can scrape from: http://host.docker.internal:" + port + "/metrics\n");
            
            System.out.println("Starting continuous test execution...");
            System.out.println("Press Ctrl+C to stop.\n");
            
            // Keep server running
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down metrics server...");
                running = false;
                if (metricsServer != null) {
                    metricsServer.stop();
                }
                System.out.println("Metrics server stopped.");
            }));
            
            int batchNumber = 0;
            
            // Continuously run test batches
            while (running) {
                batchNumber++;
                
                // Get execution date (refresh for each batch in case date changes)
                String executionDate = LocalDate.now().format(dateFormatter);
                
                System.out.println("\n==========================================");
                System.out.println("Starting Batch #" + batchNumber);
                System.out.println("Execution date: " + executionDate);
                System.out.println("==========================================\n");
                
                // Run test cases sequentially
                int successCount = 0;
                int failureCount = 0;
                
                for (int testCaseId = 1; testCaseId <= TOTAL_TEST_CASES && running; testCaseId++) {
                    // Select random API
                    String apiName = API_NAMES.get(random.nextInt(API_NAMES.size()));
                    
                    // Determine if test will pass or fail
                    boolean willPass = random.nextDouble() > FAILURE_RATE;
                    int statusCode;
                    String success;
                    
                    if (willPass) {
                        statusCode = 200;
                        success = "true";
                        successCount++;
                    } else {
                        statusCode = ERROR_CODES.get(random.nextInt(ERROR_CODES.size()));
                        success = "false";
                        failureCount++;
                    }
                    
                    // Generate random latency
                    double latency = MIN_LATENCY_SECONDS + (MAX_LATENCY_SECONDS - MIN_LATENCY_SECONDS) * random.nextDouble();
                    
                    // Simulate test execution time
                    try {
                        Thread.sleep((long)(latency * 1000));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        running = false;
                        break;
                    }
                    
                    // Record metrics
                    // Use batch number and test case ID to create unique test case identifier
                    String testCaseIdStr = String.format("batch%d_test%d", batchNumber, testCaseId);
                    
                    // Counter for total executions
                    testExecutionCounter.labels(testCaseIdStr, apiName, String.valueOf(statusCode), success, executionDate).inc();
                    
                    // Histogram for latency (for percentiles)
                    testLatency.labels(testCaseIdStr, apiName, String.valueOf(statusCode), executionDate).observe(latency);
                    
                    // Gauge for latency (plain time metric in seconds)
                    testLatencyGauge.labels(testCaseIdStr, apiName, String.valueOf(statusCode), executionDate).set(latency);
                    
                    // Success/Failure counters
                    if (willPass) {
                        testSuccessCounter.labels(testCaseIdStr, apiName, executionDate).inc();
                    } else {
                        testFailureCounter.labels(testCaseIdStr, apiName, String.valueOf(statusCode), executionDate).inc();
                    }
                    
                    // Print test result
                    String status = willPass ? "✓ PASS" : "✗ FAIL";
                    System.out.printf("Batch #%d | Test #%03d | API: %-20s | Status: %s (%d) | Latency: %.3fs | Date: %s%n",
                        batchNumber, testCaseId, apiName, status, statusCode, latency, executionDate);
                    
                    // Progress indicator every 10 tests
                    if (testCaseId % 10 == 0) {
                        System.out.printf("  Progress: %d/%d tests completed (Success: %d, Failed: %d)%n%n",
                            testCaseId, TOTAL_TEST_CASES, successCount, failureCount);
                    }
                }
                
                if (!running) {
                    break;
                }
                
                System.out.println("\n==========================================");
                System.out.println("Batch #" + batchNumber + " Summary");
                System.out.println("==========================================");
                System.out.println("Total tests: " + TOTAL_TEST_CASES);
                System.out.println("Successful: " + successCount);
                System.out.println("Failed: " + failureCount);
                System.out.println("Success rate: " + String.format("%.2f%%", (successCount * 100.0 / TOTAL_TEST_CASES)));
                System.out.println("Execution date: " + executionDate);
                System.out.println("==========================================");
                System.out.println("\nStarting next batch in 2 seconds...\n");
                
                // Small delay between batches
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running = false;
                    break;
                }
            }
            
            System.out.println("\n==========================================");
            System.out.println("Test execution stopped.");
            System.out.println("Total batches completed: " + batchNumber);
            System.out.println("Metrics are available at: http://localhost:" + port + "/metrics");
            System.out.println("==========================================");
            
            // Keep server running
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down metrics server...");
                if (metricsServer != null) {
                    metricsServer.stop();
                }
                System.out.println("Metrics server stopped.");
            }));
            
            // Keep running
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
}

