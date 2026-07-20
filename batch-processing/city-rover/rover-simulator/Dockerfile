FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy the assembled fat JAR
COPY target/scala-2.12/cityrover-rover-simulator-assembly-0.1.0.jar /app/rover-simulator.jar

# Kafka connection (override in docker-compose)
ENV KAFKA_BOOTSTRAP_SERVERS=kafka-1:19092

ENTRYPOINT ["java", "-jar", "/app/rover-simulator.jar"]
