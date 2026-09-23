FROM flink:2.3.0-scala_2.12-java17

# ------------------------------------------------------------
# Install Python + pip inside Flink image
# ------------------------------------------------------------
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------
# Create working directory for PyFlink job
# ------------------------------------------------------------
WORKDIR /opt/flink/jobs/pystreamio-flink-cassandra-test

# ------------------------------------------------------------
# Copy Python job + requirements
# ------------------------------------------------------------
COPY job/job.py ./job.py
COPY requirements.txt ./requirements.txt

# ------------------------------------------------------------
# Install Python dependencies
# ------------------------------------------------------------
RUN pip3 install --no-cache-dir -r requirements.txt

# ------------------------------------------------------------
# Copy connector JARs into Flink lib directory
# ------------------------------------------------------------
COPY jars/*.jar /opt/flink/lib/

# ------------------------------------------------------------
# Default command: run PyFlink job
# (You can override this in docker-compose)
# ------------------------------------------------------------
CMD ["flink", "run", "-py", "/opt/flink/jobs/pystreamio-flink-cassandra-test/job.py"]
