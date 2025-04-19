FROM apache/airflow:2.10.3
RUN pip3 install --upgrade pip
COPY requirements.txt .
RUN pip3 install -r requirements.txt
USER root
RUN apt-get update && apt-get install -y git && apt-get install -y curl
