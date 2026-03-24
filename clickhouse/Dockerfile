FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY feed_clickhouse.py .
CMD ["python", "feed_clickhouse.py"]