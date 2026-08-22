FROM alexflames77/custom-spark:3.5.4

LABEL maintainer="cityrover"
LABEL job="trajectory-lakehouse-writer"

ENV AWS_ACCESS_KEY_ID=minioadmin
ENV AWS_SECRET_ACCESS_KEY=minioadmin
ENV AWS_EC2_METADATA_DISABLED=true

CMD ["/opt/spark/bin/spark-submit", \
     "--master", \
     "spark://spark-master:7077", \
     "--deploy-mode", \
     "cluster", \
     "--class", \
     "cityrover.spark.lakehouse.LakehouseWriterMain", \
     "/opt/spark/lakehouse-jars/cityrover-trajectory-lakehouse-writer-job-assembly-0.1.0.jar"]
