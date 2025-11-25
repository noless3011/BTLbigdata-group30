FROM python:3.9-slim

WORKDIR /app

#Copy the data
COPY ./data/standard_log.csv /app/data/standard_log.csv

# Install curl
RUN apt-get update && apt-get install -y curl

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Remove the requirements.txt file
RUN rm /tmp/requirements.txt

COPY . /app

CMD ["sh", "-c", "bash run.sh"]