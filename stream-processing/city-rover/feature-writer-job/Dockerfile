FROM flink:2.3.0-scala_2.12-java17

WORKDIR /opt/flink/job

COPY target/scala-3.3.8/feature-writer-job-assembly-0.1.0.jar /opt/flink/job/feature-writer-job.jar

CMD ["bash", "-c", "flink run -m flink-jobmanager:8081 /opt/flink/job/feature-writer-job.jar"]
