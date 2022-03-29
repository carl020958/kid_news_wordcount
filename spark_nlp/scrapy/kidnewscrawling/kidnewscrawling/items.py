# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class KidNewScrawlingItem(scrapy.Item):
    # define the fields for your item here like:
    news_url = scrapy.Field() # 기사 원문 URL
    news_title = scrapy.Field() # 제목
    news_subtitle = scrapy.Field() # 부제목
    news_writer = scrapy.Field() # 기자
    news_date = scrapy.Field() # 날짜   
    news_article = scrapy.Field() # 기사 내용
    news_img = scrapy.Field() # 기사 img 경로
    news_source = scrapy.Field() # 신문사
#     category = scrapy.Field() # 카테고리
    pass
