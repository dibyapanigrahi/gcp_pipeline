# Consumer Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY consumer.py /app/consumer.py
COPY gcp_key.json /app/gcp_key.json

RUN pip install kafka-python google-cloud-bigquery

CMD ["python", "consumer.py"]
