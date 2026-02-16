FROM eclipse-temurin:17-jre

WORKDIR /app

COPY target/scala-2.12/geo-producer.jar /app/geo-producer.jar

ENTRYPOINT ["java", "-jar", "/app/geo-producer.jar"]

# docker build -t alexflames77/geo_producer_architecture:latest .
