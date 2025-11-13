# Redpanda Java Client

A simple Maven-based Java project to connect to a Redpanda cluster.

## Prerequisites

- Java 11 or higher
- Maven 3.6 or higher
- Redpanda cluster running (configured in docker-compose)

## Configuration

The connection details are configured in `RedpandaConnection.java` based on the rpk-profile.yaml:
- Bootstrap servers: 127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092
- SASL authentication: superuser/secretpassword
- Mechanism: SCRAM-SHA-256

## Building the Project

```bash
mvn clean compile
```

## Running the Application

```bash
mvn exec:java
```

Or compile and run manually:

```bash
mvn clean package
java -cp target/redpanda-client-1.0-SNAPSHOT.jar:target/lib/* com.example.RedpandaConnection
```

## Project Structure

```
RP/
├── pom.xml
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/
│   │   │       └── example/
│   │   │           └── RedpandaConnection.java
│   │   └── resources/
│   │       └── logback.xml
│   └── test/
│       └── java/
└── README.md
```

