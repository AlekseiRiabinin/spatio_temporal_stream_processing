FROM flink:2.3.0-scala_2.12-java17

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink/jobs/pystreamio-flink-cassandra-test

COPY job/job.py ./job.py
COPY requirements.txt ./requirements.txt

RUN python3 -m venv /opt/flink/pyenv

RUN /opt/flink/pyenv/bin/pip install --upgrade pip

# Install Python dependencies inside venv (PEP 668 compliant)
RUN /opt/flink/pyenv/bin/pip install --no-cache-dir -r requirements.txt

# Tell Flink to use this Python interpreter
ENV PYFLINK_PYTHON=/opt/flink/pyenv/bin/python
ENV PYFLINK_DRIVER_PYTHON=/opt/flink/pyenv/bin/python

COPY jars/*.jar /opt/flink/lib/

CMD ["flink", "run", "-py", "/opt/flink/jobs/pystreamio-flink-cassandra-test/job.py"]
