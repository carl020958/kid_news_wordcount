# Scrapy settings for kidnewscrawling project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'kidnewscrawling'

SPIDER_MODULES = ['kidnewscrawling.spiders']
NEWSPIDER_MODULE = 'kidnewscrawling.spiders'

# 한글깨짐
FEED_EXPORT_ENCODING = 'utf-8'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'kidnewscrawling (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# # mongodb setting
# LOG_LEVEL='ERROR'

# ITEM_PIPELINES = {'kidnewscrawling.pipelines.MongoPipeline': 300,}

# MONGO_URI = 'mongodb://root:1234@mongodb1:27017'
# MONGO_DATABASE = 'news_db'

# S3 setting
import json
from datetime import datetime, timedelta
today = datetime.strftime(datetime.now() + timedelta(hours=9), "%Y-%m-%d")

with open("/home/scrapy/scrapy/kidnewscrawling/kidnewscrawling/.credentials.json", "r") as credential:
    credential = json.load(credential)
    S3_URL = credential["S3_URL"]
    ACCESS_KEY_ID = credential["AWS_ACCESS_KEY_ID"]
    SECRET_ACCESS_KEY = credential["AWS_SECRET_ACCESS_KEY"]

ITEM_PIPELINES = {'s3pipeline.S3Pipeline': 100,}

S3PIPELINE_URL = f's3://{S3_URL}/{today}-items-{{chunk:07d}}.json'
# S3PIPELINE_URL = f's3://{S3_URL}/{{time}}-items-{{chunk:07d}}.json'

AWS_ACCESS_KEY_ID = ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = SECRET_ACCESS_KEY

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'kidnewscrawling.middlewares.KidnewscrawlingSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'kidnewscrawling.middlewares.KidnewscrawlingDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    'kidnewscrawling.pipelines.KidnewscrawlingPipeline': 300,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
