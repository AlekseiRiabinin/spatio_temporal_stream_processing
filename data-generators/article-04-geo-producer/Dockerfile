FROM eclipse-temurin:17-jre

WORKDIR /app

COPY target/scala-2.12/geo-producer.jar /app/geo-producer.jar

# ============================================================
# Kafka
# ============================================================
ENV KAFKA_BOOTSTRAP_SERVERS=kafka-1:19092
ENV KAFKA_TOPIC=spatial-events

# ============================================================
# Rate Control
# ============================================================
ENV EVENT_RATE_PATTERN=constant
ENV EVENT_RATE=50

ENV BURST_RATE=100
ENV BURST_DURATION_MS=10000
ENV PAUSE_MS=5000

ENV WAVE_MIN=5
ENV WAVE_MAX=100
ENV WAVE_PERIOD_MS=10000

# ============================================================
# Geometry / Spatial Distribution
# ============================================================
ENV GEOMETRY_PATTERN=clustered

# ============================================================
# Motion Model
# ============================================================
ENV MOTION_MODE=swarm

# ============================================================
# Stream Scale
# ============================================================
ENV KEYS=100

# ============================================================
# Base Timestamp Pattern
# ============================================================
ENV TIMESTAMP_PATTERN=realtime
ENV MAX_SKEW_MS=2000
ENV MAX_DELAY_MS=5000

ENV DISORDER_PROBABILITY=0.20
ENV MAX_DISORDER_MS=3000

# ============================================================
# Additional Timestamp Skew Injection
# ============================================================
ENV ENABLE_TIMESTAMP_SKEW=false
ENV TIMESTAMP_SKEW_MODE=bidirectional
ENV SKEW_PROBABILITY=0.20

# ============================================================
# Composite Disorder Pipeline
# ============================================================
ENV ENABLE_DISORDER=false

ENV ENABLE_JITTER=false
ENV MAX_JITTER_MS=500

ENV ENABLE_BURST_DELAY=false
ENV BURST_DELAY_PROBABILITY=0.05
ENV BURST_DELAY_MS=10000

ENV ENABLE_GPS_DROPOUT=false
ENV GPS_DROPOUT_PROBABILITY=0.00

# ============================================================
# Article 04 Adaptive Layer
# ============================================================
ENV WINDOW_STRATEGY=fixed
ENV WINDOW_SIZE_MS=5000

ENV WATERMARK_STRATEGY=fixed
ENV WATERMARK_DELAY_MS=3000

ENV ML_INFERENCE=disabled
ENV MODEL_UPDATE_INTERVAL=5000

ENV ADAPTATION_INTERVAL_MS=2000

ENTRYPOINT ["java", "-jar", "/app/geo-producer.jar"]


# docker build -t alexflames77/geo_producer_adaptive_control:latest .
