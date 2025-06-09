FROM apache/airflow:3.0.1
ADD requirements.txt .
RUN pip install apache-airflow==3.0.1 -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.1/constraints-3.12.txt"