FROM eclipse-temurin:17-jre

WORKDIR /app

COPY target/scala-2.12/geo-producer.jar /app/geo-producer.jar

ENTRYPOINT ["java", "-cp", "geo-producer.jar", "GeoEventProducer"]

# docker build -t alexflames77/geo_producer_architecture:latest .
