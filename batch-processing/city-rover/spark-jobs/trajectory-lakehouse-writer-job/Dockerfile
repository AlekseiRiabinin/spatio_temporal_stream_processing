FROM alexflames77/custom-spark:3.5.4

LABEL maintainer="cityrover"
LABEL job="trajectory-lakehouse-writer"

# ------------------------------------------------------------
# Create directory for job JARs
# ------------------------------------------------------------

RUN mkdir -p /opt/spark/city-rover-jars

# ------------------------------------------------------------
# Copy application assembly JAR
# ------------------------------------------------------------

COPY target/scala-2.12/cityrover-trajectory-lakehouse-writer-job-assembly-0.1.0.jar \
     /opt/spark/city-rover-jars/

# ------------------------------------------------------------
# MinIO development credentials
#
# For production these should come from secrets/environment.
# ------------------------------------------------------------

ENV AWS_ACCESS_KEY_ID=minioadmin
ENV AWS_SECRET_ACCESS_KEY=minioadmin
ENV AWS_EC2_METADATA_DISABLED=true

# ------------------------------------------------------------
# Spark submit
# ------------------------------------------------------------

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--deploy-mode", "cluster", \
     "--class", "cityrover.spark.lakehouse.LakehouseWriterMain", \
     "/opt/spark/city-rover-jars/cityrover-trajectory-lakehouse-writer-job-assembly-0.1.0.jar"]
