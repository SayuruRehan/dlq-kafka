#!/bin/bash

echo "Starting DLQ Kafka Consumer Application..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker first."
    exit 1
fi

# Start Kafka infrastructure
echo "Starting Kafka infrastructure with Docker Compose..."
docker-compose up -d

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 30

# Build the application
echo "Building the application..."
mvn clean package -DskipTests

# Run the application
echo "Starting the application..."
java -jar target/dlq-kafka-1.0.0.jar

echo "Application started! Access the DLQ dashboard at: http://localhost:8080/dlq"
echo "Kafka UI is available at: http://localhost:8080 (if not conflicting with app port)"
