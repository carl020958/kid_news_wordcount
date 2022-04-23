from airflow.models import DAG
from airflow.models import Variable
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python import ShortCircuitOperator
import boto3
from datetime import datetime, timedelta

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

def s3_data_load():
    """check whether data was crawled"""
    s3_client = boto3.client(
        "s3", 
        aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    )
    today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")
    Bucket, path = Variable.get("Bucket"), f'{Variable.get("kid_news_dir")}/{today}'

    res = s3_client.list_objects_v2(Bucket=Bucket, Prefix=path, MaxKeys=1)
    return 'Contents' in res

templated_bash_command_pyspark = """
    {{params.spark_submit}} \
    --master {{params.master}} \
    --deploy-mode {{params.deploy_mode}} \
    --num-executors {{params.num_executors}} \
    --executor-cores {{params.executor_cores}} \
    --executor-memory {{params.executor_memory}} \
    --conf {{params.conf1}} \
    --conf {{params.conf2}} \
    --conf {{params.conf3}} \
    --jars {{params.jars}} \
    {{params.application}}
"""

with DAG(
    "kid_news_wordcount_v2",
    schedule_interval="30 14 * * *",
    default_args=default_args,
    catchup=False,
    params={
        # params for scrapy
        "user": "scrapy",
        "scrapy_env_dir": "/home/scrapy",
        "scrapy_project_dir": "/home/scrapy/scrapy/kidnewscrawling/kidnewscrawling",
        "spider": "kidNewsSpiderCurrentAffairs",

        # params for spark
        "spark_submit": "/opt/spark/bin/spark-submit",
        "master": "yarn",
        "deploy_mode": "client",
        "num_executors": "2",
        "executor_cores": "2",
        "executor_memory": "2048m",
        "conf1": "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "conf2": f'spark.hadoop.fs.s3a.access.key={Variable.get("AWS_ACCESS_KEY_ID")}',
        "conf3": f'spark.hadoop.fs.s3a.secret.key={Variable.get("AWS_SECRET_ACCESS_KEY")}',
        "jars": Variable.get("wordcount_jars_v2"),
        "application": "/opt/workspace/src/kid_word_count_batch_s3.py"
    }

) as dag:
    # task for kid news scraping
    kid_news_scrapy = SSHOperator(
        task_id="kid_news_scrapy",
        ssh_conn_id="ssh_scrapy",
        command=templated_bash_command_scrapy,
        dag=dag
    )

    # task to check whether data exists
    check_s3 = ShortCircuitOperator(
        task_id="check_s3",
        python_callable=s3_data_load,
        dag=dag)

    # task for kid wordcount
    kid_wordcount = SSHOperator(
        task_id="kid_wordcount",
        ssh_conn_id="ssh_spark_yarn",
        command=templated_bash_command_pyspark,
        dag=dag
    )

kid_news_scrapy >> check_s3 >> kid_wordcount
