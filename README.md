## News_wordcount

### Data-Flow
<p align="center">
    <img width="677" alt="data_flow" src="https://user-images.githubusercontent.com/77782770/162112185-4efe06f9-71d7-441e-8de4-1319d0b6a3bb.png" align="center">
</p>

### Mariadb
```bash
CREATE DATABASE news_db;
USE news_db;

CREATE TABLE IF NOT EXISTS kid_word_count(
    count_id INT AUTO_INCREMENT PRIMARY KEY,
    count_date DATE NOT NULL,
    count_word VARCHAR(100) NOT NULL,
    count_value INT NOT NULL
);

CREATE TABLE IF NOT EXISTS adult_word_count(
    count_id INT AUTO_INCREMENT PRIMARY KEY,
    count_date DATE NOT NULL,
    count_word VARCHAR(100) NOT NULL,
    count_value INT NOT NULL
);
```

### Spark Standalone Pyspark-shell
```bash
$SPARK_HOME/bin/pyspark \
--master spark://spark-master:7077 \
--num-executors 2 \
--executor-cores 2 \
--executor-memory "2048m" \
--jars \
/opt/workspace/jars/bson-4.0.5.jar,\
/opt/workspace/jars/mongo-spark-connector_2.12-3.0.1.jar,\
/opt/workspace/jars/mongodb-driver-core-4.0.5.jar,\
/opt/workspace/jars/mongodb-driver-sync-4.0.5.jar,\
/opt/workspace/jars/mysql-connector-java-8.0.21.jar
```

### Spark on YARN Pyspark-shell
```bash
$SPARK_HOME/bin/pyspark \
--master yarn \
--deploy-mode client \
--num-executors 2 \
--executor-cores 2 \
--executor-memory "2048m" \
--jars \
/opt/workspace/jars/bson-4.0.5.jar,\
/opt/workspace/jars/mongo-spark-connector_2.12-3.0.1.jar,\
/opt/workspace/jars/mongodb-driver-core-4.0.5.jar,\
/opt/workspace/jars/mongodb-driver-sync-4.0.5.jar,\
/opt/workspace/jars/mysql-connector-java-8.0.21.jar
```

### Spark & S3
$SPARK_HOME/bin/pyspark \
--master yarn \
--deploy-mode client \
--num-executors 2 \
--executor-cores 2 \
--executor-memory "2048m" \
--conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
--conf "spark.hadoop.fs.s3a.access.key=[Access_Key_ID]]" \
--conf "spark.hadoop.fs.s3a.secret.key=[Secret_Access_Key]" \
--jars \
/opt/workspace/jars/hadoop-aws-3.2.3.jar,\
/opt/workspace/jars/aws-java-sdk-bundle-1.11.901.jar,\
/opt/workspace/jars/mysql-connector-java-8.0.21.jar