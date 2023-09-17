FROM bitnami/spark:3.3.2

USER root

COPY connections.yml /airflow/connections.yml

RUN pip3 install apache-airflow==2.7.1 apache-airflow-providers-apache-spark

RUN airflow users create -u airflow -p airflow -f Peter -l Parker -r Admin -e airflow

RUN airflow connections import /airflow/connections.yml