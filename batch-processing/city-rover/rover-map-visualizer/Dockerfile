FROM eclipse-temurin:17-jre

WORKDIR /opt/cityrover

COPY target/scala-2.12/rover-map-visualizer-assembly-0.1.0.jar app.jar

EXPOSE 8080

ENV JAVA_OPTS="-Xmx512m"
ENV VISUALIZER_OUTPUT_DIR="/opt/cityrover/graph-engine/output"
ENV VISUALIZER_WEBROOT="web"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
