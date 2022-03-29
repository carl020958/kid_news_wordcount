from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
# from airflow.providers.apache.spark.operators import spark_submit
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 21),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

templated_bash_command = """/usr/bin/spark-3.1.2-bin-hadoop3.2/bin/spark-submit \
--master spark://spark-master:7077 \
--num-executors 3 \
--executor-cores 2 \
--executor-memory 3072m \
--jars /opt/workspace/jars/bson-4.0.5.jar,\
/opt/workspace/jars/mongo-spark-connector_2.12-3.0.1.jar,\
/opt/workspace/jars/mongodb-driver-core-4.0.5.jar,\
/opt/workspace/jars/mongodb-driver-sync-4.0.5.jar,\
/opt/workspace/jars/mysql-connector-java-8.0.21.jar \
/opt/workspace/scripts/word_count_dump.py"""

# mongo_jar1 = "/home/airflow/airflow/spark/jars/bson-4.0.5.jar,"
# mongo_jar2 = "/home/airflow/airflow/spark/jars/mongo-spark-connector_2.12-3.0.1.jar,"
# mongo_jar3 = "/home/airflow/airflow/spark/jars/mongodb-driver-core-4.0.5.jar,"
# mongo_jar4 = "/home/airflow/airflow/spark/jars/mongodb-driver-sync-4.0.5.jar,"
# mysql_jar = "/home/airflow/airflow/spark/jars/mysql-connector-java-8.0.21.jar"

with DAG(
    'kid_news_wordcount',
    schedule_interval='45 14 * * *',
    default_args=default_args,
    catchup=False,
) as dag:
    news_scrapy = BashOperator(
        task_id="kid_news_scrapy",
        bash_command="sh /home/airflow/scrapy/kidnewscrawling/scrape.sh "
    )
    # wordcount = SparkSubmitOperator(
    #     task_id="wordcount",
    #     conn_id="spark_standalone",
    #     application="/home/airflow/airflow/spark/applications/word_count_dump.py",
    #     total_executor_cores="6",
    #     executor_cores="2",
    #     executor_memory="3072m",
    #     num_executors="3",
    #     name="spark-wordcount",
    #     verbose=False,
    #     driver_memory="2g",
    #     jars=mongo_jar1 + mongo_jar2 + mongo_jar3 + mongo_jar4 + mysql_jar,
    #     dag=dag
    # )
    
    wordcount = SSHOperator(
        task_id="wordcount",
        ssh_conn_id="ssh_spark",
        command=templated_bash_command,
        dag=dag
    )

news_scrapy >> wordcount
