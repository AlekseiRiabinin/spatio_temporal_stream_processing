FROM alexflames77/custom-spark:3.5.4

LABEL maintainer="cityrover"
LABEL job="trajectory-visualizer"

RUN mkdir -p /opt/spark/city-rover-jars

COPY target/scala-2.12/cityrover-trajectory-visualizer-job-assembly-0.1.0.jar \
     /opt/spark/city-rover-jars/

CMD ["/opt/spark/bin/spark-submit", \
     "--master", \
     "spark://spark-master:7077", \
     "--deploy-mode", \
     "cluster", \
     "--class", \
     "cityrover.spark.trajectory.TrajectoryVisualizerMain", \
     "/opt/spark/city-rover-jars/cityrover-trajectory-visualizer-job-assembly-0.1.0.jar"]
