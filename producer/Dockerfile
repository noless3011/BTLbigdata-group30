FROM python:3.9-slim

WORKDIR /app

COPY ./producer.py /app/producer.py
COPY ./data/standard_log.csv /app/data/standard_log.csv

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Remove the requirements.txt file
RUN rm /tmp/requirements.txt

CMD [ "python", "-u", "producer.py", "--mode", "setup", "--bootstrap_servers", "debezium-cluster-kafka-bootstrap:9092"]
