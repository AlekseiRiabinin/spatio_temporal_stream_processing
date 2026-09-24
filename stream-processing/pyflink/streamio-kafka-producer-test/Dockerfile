FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_producer.py kafka_producer.py

ENV KAFKA_BOOTSTRAP_SERVERS=kafka-1:19092
ENV PYTHONUNBUFFERED=1

CMD ["python", "kafka_producer.py"]
