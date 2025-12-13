# Dockerfile for Serving Layer (API + UI)
FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY serving_layer.py .
COPY dashboard.py .

# Expose ports (8000 for API, 8501 for Streamlit)
EXPOSE 8000
EXPOSE 8501

# Default command (can be overridden)
CMD ["uvicorn", "serving_layer:app", "--host", "0.0.0.0", "--port", "8000"]
