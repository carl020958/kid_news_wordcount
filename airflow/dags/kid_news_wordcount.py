from airflow.models import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 29),
    'catchup': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

templated_bash_command_scrapy = """
    su - {{params.user}}
    cd {{params.scrapy_env_dir}}
    source ./.venv/bin/activate
    cd {{params.scrapy_project_dir}}
    scrapy crawl {{params.spider}}
""" 

templated_bash_command_pyspark = """
    {{params.spark_submit}} \
    --master {{params.master}} \
    --num-executors {{params.num_executors}} \
    --executor-cores {{params.executor_cores}} \
    --executor-memory {{params.executor_memory}} \
    --jars {{params.jars}} \
    {{params.application}}
"""

with DAG(
    "kid_news_wordcount",
    schedule_interval="45 14 * * *",
    default_args=default_args,
    catchup=False,
    params={
        # params for scrapy
        "user": "scrapy",
        "scrapy_env_dir": "/home/scrapy",
        "scrapy_project_dir": "/home/scrapy/scrapy/kidnewscrawling/kidnewscrawling",
        "spider": "kidNewsSpiderCurrentAffairs",

        # params for spark
        "spark_submit": "/usr/bin/spark-3.1.2-bin-hadoop3.2/bin/spark-submit",
        "master": "spark://spark-master:7077",
        "num_executors": "3",
        "executor_cores": "2",
        "executor_memory": "3072m",
        "jars": Variable.get("wordcount_jars"),
        "application": "/opt/workspace/scripts/kid_word_count_batch.py"
    }

) as dag:
    # task for kid news scraping
    kid_news_scrapy = SSHOperator(
        task_id="kid_news_scrapy",
        ssh_conn_id="ssh_scrapy",
        command=templated_bash_command_scrapy,
        dag=dag
    )

    # task for kid wordcount
    kid_wordcount = SSHOperator(
        task_id="kid_wordcount",
        ssh_conn_id="ssh_spark",
        command=templated_bash_command_pyspark,
        dag=dag
    )

kid_news_scrapy >> kid_wordcount
