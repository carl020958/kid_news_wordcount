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

templated_bash_command_naver_news_crawl = """
    su - {{params.user}}
    cd {{params.naver_news_env_dir}}
    source ./.venv/bin/activate
    mkdir tmp
    python3 naver_news_crawler.py
"""

templated_bash_command_naver_news_load = """
    su - {{params.user}}
    cd {{params.naver_news_env_dir}}
    source ./.venv/bin/activate
    python3 json_to_mongo.py
    rm -rf {{params.json_file_dir}}
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
    "adult_news_wordcount",
    schedule_interval="15 15 * * *",
    default_args=default_args,
    catchup=False,
    params={
        # params for naver_news_crawling
        "user": "scrapy",
        "naver_news_env_dir": "/home/scrapy/adult_news",
        "json_file_dir": "/home/scrapy/adult_news/tmp",

        # params for spark
        "spark_submit": "/usr/bin/spark-3.1.2-bin-hadoop3.2/bin/spark-submit",
        "master": "spark://spark-master:7077",
        "num_executors": "2",
        "executor_cores": "1",
        "executor_memory": "2048m",
        "jars": Variable.get("wordcount_jars"),
        "application": "/opt/workspace/scripts/adult_word_count_batch.py"
    }

) as dag:
    # task for naver news crawling
    adult_news_crawler = SSHOperator(
        task_id="adult_news_crawler",
        ssh_conn_id="ssh_scrapy",
        command=templated_bash_command_naver_news_crawl,
        dag=dag
    )

    # task for naver news load
    adult_news_load = SSHOperator(
        task_id="adult_news_load",
        ssh_conn_id="ssh_scrapy",
        command=templated_bash_command_naver_news_load,
        dag=dag
    )

    # task for adult wordcount
    adult_wordcount = SSHOperator(
        task_id="adult_wordcount",
        ssh_conn_id="ssh_spark",
        command=templated_bash_command_pyspark,
        dag=dag
    )

adult_news_crawler >> adult_news_load >> adult_wordcount

