FROM flink:1.17.1

WORKDIR /opt/flink/usrlib

COPY target/scala-2.12/article-02-stream-models.jar /opt/flink/usrlib/article-02-stream-models.jar

ENV FLINK_MAIN_CLASS=phd.streammodels.Article02StreamModelsJob

EXPOSE 9000

CMD ["bash", "-c", "flink run -c $FLINK_MAIN_CLASS /opt/flink/usrlib/article-02-stream-models.jar"]
