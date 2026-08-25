FROM flink:2.3.0-scala_2.12-java17

WORKDIR /opt/flink/job

# Copy your manually built fat JAR
COPY target/scala-3.3.8/rover-flink-latency-assembly-0.1.0.jar /opt/flink/job/job.jar

# Default command: submit job to JobManager
CMD ["bash", "-c", "flink run -m flink-jobmanager:8081 /opt/flink/job/job.jar"]
