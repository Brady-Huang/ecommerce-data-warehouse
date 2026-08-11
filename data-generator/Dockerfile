FROM python:3.11-slim

WORKDIR /app

COPY data-generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY data-generator/ .

CMD ["python","generate.py"]