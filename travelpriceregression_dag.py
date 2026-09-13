from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.data_loader import load_flight_data
from src.preprocessing import preprocess_data
from src.train import train_model
from src.model import evaluate_model

import os
import subprocess

default_args = {
    "owner": "bharat",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def deploy_to_kubernetes():
    subprocess.run(
        ["kubectl", "rollout", "restart", "deployment/price-predictor"],
        check=True
    )

with DAG(
    dag_id="travel_price_regression_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    default_args=default_args
) as dag:

    load_data = PythonOperator(
        task_id="load_travel_data",
        python_callable=load_flight_data
    )

    preprocess = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data
    )

    train = PythonOperator(
        task_id="train_regression_model",
        python_callable=train_model
    )

    evaluate = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model
    )

    deploy = PythonOperator(
        task_id="deploy_model",
        python_callable=deploy_to_kubernetes
    )

    load_data >> preprocess >> train >> evaluate >> deploy
