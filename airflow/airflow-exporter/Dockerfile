FROM python:3.10-slim

WORKDIR /app

RUN pip install prometheus-flask-exporter prometheus_client requests flask python-dateutil psutil

COPY exporter.py .
EXPOSE 9112
CMD ["python", "exporter.py"]