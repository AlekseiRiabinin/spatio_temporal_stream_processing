FROM flink:1.17.1

WORKDIR /opt/flink/usrlib

COPY target/scala-2.12/article-03-spatial-methods.jar /opt/flink/usrlib/article-03-spatial-methods.jar

ENV FLINK_MAIN_CLASS=phd.spatialmethods.Article03SpatialMethodsJob

EXPOSE 9000

CMD ["bash", "-c", "flink run -c $FLINK_MAIN_CLASS /opt/flink/usrlib/article-03-spatial-methods.jar"]
