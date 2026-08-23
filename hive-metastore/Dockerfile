FROM apache/hive:3.1.3

USER root

# ================================================================
# System dependencies
# ================================================================

RUN apt-get update && apt-get install -y \
    wget \
    ca-certificates \
    postgresql-client \
    curl \
    netcat-openbsd \
  && rm -rf /var/lib/apt/lists/*


# ================================================================
# Hadoop configuration
#
# Keep Hadoop aligned with Spark 3.5.4:
# Hadoop 3.3.4
# ================================================================

ENV HIVE_HOME=/opt/hive \
    HIVE_LIB_DIR=/opt/hive/lib \
    HADOOP_VERSION=3.3.4 \
    HADOOP_HOME=/opt/hadoop \
    PATH=/opt/hive/bin:/opt/hadoop/bin:$PATH


# ================================================================
# Install Hadoop 3.3.4
# ================================================================

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
  && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt \
  && ln -s /opt/hadoop-${HADOOP_VERSION} $HADOOP_HOME \
  && rm hadoop-${HADOOP_VERSION}.tar.gz


# ================================================================
# Remove potentially conflicting Hadoop/AWS libraries
# ================================================================

RUN find $HIVE_LIB_DIR -name 'hadoop-common-*.jar' -exec rm -f {} \; \
  && find $HIVE_LIB_DIR -name 'hadoop-aws-*.jar' -exec rm -f {} \; \
  && find $HIVE_LIB_DIR -name 'aws-java-sdk-bundle-*.jar' -exec rm -f {} \;


# ================================================================
# Install Hadoop libraries required for S3A / MinIO
# ================================================================

RUN cp $HADOOP_HOME/share/hadoop/common/hadoop-common-*.jar $HIVE_LIB_DIR/ \
  && cp $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-*.jar $HIVE_LIB_DIR/ \
  && cp $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar $HIVE_LIB_DIR/


# ================================================================
# PostgreSQL JDBC driver
# ================================================================

RUN wget -P $HIVE_LIB_DIR \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar


# ================================================================
# Logging
# ================================================================

RUN echo "log4j.rootLogger=INFO, console" > $HIVE_HOME/conf/log4j.properties \
  && echo "log4j.appender.console=org.apache.log4j.ConsoleAppender" >> $HIVE_HOME/conf/log4j.properties \
  && echo "log4j.appender.console.layout=org.apache.log4j.PatternLayout" >> $HIVE_HOME/conf/log4j.properties \
  && echo "log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n" >> $HIVE_HOME/conf/log4j.properties


# ================================================================
# Hive Metastore initialization marker
# ================================================================

RUN mkdir -p /var/hive \
  && chown hive:hive /var/hive


# ================================================================
# Entrypoint
# ================================================================

COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

USER hive

ENTRYPOINT ["/entrypoint.sh"]
