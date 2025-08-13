# Sử dụng image Airflow chính thức làm base
FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean

# Trở lại user airflow
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
COPY requirements.txt .
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --no-cache-dir -r requirements.txt

