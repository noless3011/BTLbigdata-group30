FROM python:3.10-slim

# Prevent Python buffering issues
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (layer caching)
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY producer/producer.py .

# Default command
CMD ["python", "producer.py"]
