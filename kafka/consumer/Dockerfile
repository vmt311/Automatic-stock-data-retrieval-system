# consumer/Dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY setup.txt .
RUN pip install --no-cache-dir -r setup.txt
COPY consumer.py .
CMD ["python", "consumer.py"]