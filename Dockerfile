FROM apache/airflow:2.10.3
RUN pip3 install --upgrade pip
RUN pip3 install hydra-core
RUN pip3 install mmcif
RUN pip3 install requests
RUN pip3 install pymongo
RUN pip3 install rcsb.utils.io
USER root
RUN apt-get update && apt-get install -y git && apt-get install -y curl
ENV PYTHONPATH=$PYTHONPATH:/py-rcsb_workflow
ENV PYTHONPATH=$PYTHONPATH:/py-rcsb_db
