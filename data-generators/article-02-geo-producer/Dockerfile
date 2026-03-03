FROM eclipse-temurin:17-jre

WORKDIR /app

COPY target/scala-2.12/geo-producer.jar /app/geo-producer.jar

ENV KAFKA_BOOTSTRAP_SERVERS=kafka-1:19092
ENV KAFKA_TOPIC=spatial-events

# Rate patterns
ENV EVENT_RATE_PATTERN=constant
ENV EVENT_RATE=50
ENV BURST_RATE=100
ENV BURST_DURATION_MS=10000
ENV PAUSE_MS=5000
ENV WAVE_MIN=5
ENV WAVE_MAX=100
ENV WAVE_PERIOD_MS=10000

# Geometry patterns
ENV GEOMETRY_PATTERN=random

# Timestamp patterns
ENV TIMESTAMP_PATTERN=realtime
ENV MAX_SKEW_MS=2000
ENV MAX_DELAY_MS=5000

# Number of keys (vehicles)
ENV KEYS=10

ENTRYPOINT ["java", "-jar", "/app/geo-producer.jar"]


# docker build -t alexflames77/geo_producer_stream_models:latest .
