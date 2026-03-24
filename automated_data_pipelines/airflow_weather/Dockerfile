FROM apache/airflow:3.1.8

USER root
RUN apt-get update && apt-get install -y build-essential && apt-get clean

USER airflow

WORKDIR /opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt