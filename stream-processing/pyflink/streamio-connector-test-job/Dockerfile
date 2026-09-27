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

# --- PyFlink interpreter configuration -------------------------------------
# Flink's Python driver looks for a binary named "python" (not "python3").
# Debian provides only /usr/bin/python3, so we must:
#   1) symlink python -> venv interpreter on the PATH (client + worker fallback)
#   2) point Flink at the venv explicitly via the supported env var

# 1) Make "python" resolvable on the default PATH (-> venv interpreter)
RUN ln -s /opt/flink/pyenv/bin/python /usr/local/bin/python

# 2) Tell PyFlink which interpreter to use for the client (job submission)
ENV PYFLINK_CLIENT_EXECUTABLE=/opt/flink/pyenv/bin/python

# --- Flink connector jars --------------------------------------------------
COPY jars/*.jar /opt/flink/lib/

CMD ["flink", "run", "-py", "/opt/flink/jobs/pystreamio-flink-cassandra-test/job.py"]
