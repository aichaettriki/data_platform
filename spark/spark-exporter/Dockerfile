FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy exporter script
COPY exporter.py .

# Expose metrics port
EXPOSE 9091

# Run exporter
CMD ["python", "exporter.py"]
