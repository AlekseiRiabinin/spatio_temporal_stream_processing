FROM flink:2.3.0-scala_2.12-java17

# Remove Flink's bundled Scala 2.12 libraries. Flink 2.3 dropped the Scala API
# in favour of Java-only APIs, so these are not needed by Java API jobs and
# conflict with the Scala 3 library shaded into our job jar (they contain
# scala.Predef$, which shadows our Scala 3 version → NoSuchMethodError).
RUN find /opt/flink/lib -maxdepth 1 -name 'scala-library*.jar' -delete && \
    find /opt/flink/lib -maxdepth 1 -name 'flink-scala_2.12*.jar' -delete

WORKDIR /opt/flink/job

COPY target/scala-3.3.8/feature-transform-job-assembly-0.1.0.jar /opt/flink/job/feature-transform-job.jar

CMD ["bash", "-c", "flink run -m flink-jobmanager:8081 /opt/flink/job/feature-transform-job.jar"]
