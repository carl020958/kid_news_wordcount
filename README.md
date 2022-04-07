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

### Pyspark-shell(for debugging)
```bash
$SPARK_HOME/bin/pyspark \
--master spark://spark-master:7077 \
--num-executors 3 \
--executor-cores 2 \
--executor-memory "3072m" \
--jars \
/opt/workspace/jars/bson-4.0.5.jar,\
/opt/workspace/jars/mongo-spark-connector_2.12-3.0.1.jar,\
/opt/workspace/jars/mongodb-driver-core-4.0.5.jar,\
/opt/workspace/jars/mongodb-driver-sync-4.0.5.jar,\
/opt/workspace/jars/mysql-connector-java-8.0.21.jar
```