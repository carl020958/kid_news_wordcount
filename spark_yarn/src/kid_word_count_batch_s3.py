from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from konlpy.tag import Mecab
import pyspark.sql.functions as F
from pyspark.sql import types as T
from typing import List, Set
from datetime import datetime, timedelta
import re
import json
m = Mecab()

@F.udf(T.ArrayType(T.StringType()))
def ko_tokenize(article:str) -> List[str]:
    """return words from the article with a token matching the pattern and not included in the stopwords"""
    # get stop words
    with open("/opt/workspace/stop_words/korean_stop_words.txt", 'r') as f:
        stopwords = {re.sub("\n", "", line) for line in f.readlines()}

    pattern = r"(NN|XR|VA|VV)"
    return [word for word,tag in m.pos(article) if re.search(pattern, tag) and word not in stopwords]


if __name__ == "__main__":
    
    # setting for spark-submit
    spark = SparkSession \
        .builder \
        .appName("kid_wordcount") \
        .getOrCreate()
        
    # get credential data
    with open("/opt/workspace/src/.credentials.json", "r") as credential:
        credential = json.load(credential)
        S3_URL = credential["S3_URL"]
        AWS_RDS_URL = credential["AWS_RDS_URL"]
        AWS_RDS_USER = credential["AWS_RDS_USER"]
        AWS_RDS_PASSWORD = credential["AWS_RDS_PASSWORD"]

    # load data
    today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")
    df = spark.read.json(f"s3a://{S3_URL}/{today}*.json")

    # apply function
    token_df = df \
        .withColumn("article_words_list", ko_tokenize(df.news_article)) \
        .withColumn("news_date", df.news_date.cast("date"))
        
    # article_words_list to rows
    kid_word_count = token_df \
        .select("news_date", "article_words_list") \
        .withColumn("article_word", F.explode("article_words_list")) \
        .groupBy("news_date", "article_word") \
        .count() \
        .orderBy("news_date", "count")
        
    # make kid_count_id as null for pk, rename columns
    # spark-submit - F.lit(None).cast("string") / pyspark shell - F.lit(0)
    kid_word_count = kid_word_count \
        .withColumn("id", F.lit(None).cast("string")) \
        .withColumnRenamed("news_date", "date") \
        .withColumnRenamed("article_word", "word") \
        .withColumnRenamed("count", "count") \
        .select("id", "date", "word", "count")

    # export data to MySQL
    kid_word_count.write.format("jdbc") \
        .mode("append") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", AWS_RDS_URL) \
        .option("dbtable", "word_count") \
        .option("user", AWS_RDS_USER) \
        .option("password", AWS_RDS_PASSWORD) \
        .save()


