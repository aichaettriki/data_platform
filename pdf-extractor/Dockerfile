FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    libgl1 libglib2.0-0 poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install \
    "mineru[pipeline]" \
    flask \
    pandas \
    openpyxl

COPY app.py .

EXPOSE 7861
CMD ["python", "app.py"]