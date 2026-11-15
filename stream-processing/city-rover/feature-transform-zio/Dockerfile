FROM eclipse-temurin:25-jre-jammy

WORKDIR /app

COPY target/scala-3.9.0/feature-transform-zio-assembly-0.1.0-SNAPSHOT.jar /app/app.jar

EXPOSE 9084

ENTRYPOINT ["java", "-jar", "/app/app.jar"]