FROM apache/airflow:2.10.3
RUN pip3 install --upgrade pip
USER root
RUN apt-get update && apt-get install -y git && apt-get install -y curl
RUN mkdir /app
COPY . /app
WORKDIR /app/py-rcsb_workflow
RUN pip3 install -r requirements.txt 
WORKDIR /app/dags
RUN pip3 install .
WORKDIR /app

