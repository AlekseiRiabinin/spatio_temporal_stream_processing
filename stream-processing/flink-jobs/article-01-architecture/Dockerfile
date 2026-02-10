FROM flink:1.17.1

WORKDIR /opt/flink/usrlib

COPY target/scala-2.12/article-01-architecture.jar /opt/flink/usrlib/article-01-architecture.jar

ENV FLINK_MAIN_CLASS=phd.architecture.Article01ArchitectureJob
ENV FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager"

CMD ["bash", "-c", "flink run -c $FLINK_MAIN_CLASS /opt/flink/usrlib/article-01-architecture.jar"]


# docker compose -f ../../../docker/docker-compose.yml build geoflink_architecture_job
