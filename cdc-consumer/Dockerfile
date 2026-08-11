FROM python:3.11-slim

WORKDIR /app

COPY cdc-consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cdc-consumer/ .

CMD ["python", "consumer.py"]