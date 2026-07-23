FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy the assembled fat JAR
COPY target/scala-2.12/cityrover-graph-engine-assembly-0.1.0.jar /app/graph-engine.jar

# PostGIS connection parameters (override in docker-compose)
ENV POSTGIS_HOST=postgis-real-deal
ENV POSTGIS_PORT=5432
ENV POSTGIS_DB=cre_db
ENV POSTGIS_USER=cre_user
ENV POSTGIS_PASSWORD=cre_password

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/graph-engine.jar"]
