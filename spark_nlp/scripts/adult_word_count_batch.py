from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from konlpy.tag import Mecab
import pyspark.sql.functions as F
from pyspark.sql import types as T
from typing import List, Set
from datetime import datetime, timedelta
import re
m = Mecab()

@F.udf(T.ArrayType(T.StringType()))
def ko_tokenize(article:str) -> List[str]:
    """return words from the article with a token matching the pattern and not included in the stopwords"""
    # get stop words
    with open("/opt/workspace/stop_words/korean_stop_words.txt", 'r') as f:
        stopwords = {re.sub("\n", "", line) for line in f.readlines()}

    pattern = r'(NN|XR|VA|VV)'
    return [word for word,tag in m.pos(article) if re.search(pattern, tag) and word not in stopwords]

if __name__ == "__main__":
    
    # setting for spark-submit
    spark = SparkSession \
        .builder \
        .appName("adult_wordcount") \
        .getOrCreate()

    # load data
    today = (datetime.today() - timedelta(1)).replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + ".000Z"
    pipeline = '{"$match": {"news_date": {"$gte": ISODate("%s")}}}' % today
    df = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri","mongodb://root:1234@mongodb1:27017")\
        .option("database","news_db")\
        .option("collection", "adult_news")\
        .option("pipeline", pipeline)\
        .load()

    # apply function
    token_df = df \
        .withColumn('article_words_list', ko_tokenize(df.news_article)) \
        .withColumn("news_date", F.to_date("news_date", "yyyy-MM-dd"))
        
    # article_words_list to rows
    adult_word_count = token_df \
        .select('news_date', 'article_words_list') \
        .withColumn('article_word', F.explode('article_words_list')) \
        .groupBy("news_date", 'article_word') \
        .count() \
        .orderBy("news_date", 'count')
        
    # make kid_count_id as null for pk, rename columns
    # spark-submit - F.lit(None).cast('string') / pyspark shell - F.lit(0)
    adult_word_count = adult_word_count \
        .withColumn('count_id', F.lit(None).cast('string')) \
        .withColumnRenamed("news_date", "count_date") \
        .withColumnRenamed("article_word", "count_word") \
        .withColumnRenamed("count", "count_value") \
        .select("count_id", "count_date", "count_word", "count_value")
        
    # export data to MySQL
    adult_word_count.write.format('jdbc') \
        .mode('append') \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://mariadb1:3306/news_db?serverTimezone=UTC&useSSL=false") \
        .option("dbtable", "adult_word_count") \
        .option("user", "root") \
        .option("password", "1234") \
        .save()
