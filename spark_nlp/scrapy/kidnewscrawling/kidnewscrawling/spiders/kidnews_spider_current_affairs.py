# -*- coding: utf-8 -*-

import scrapy
from kidnewscrawling.items import KidNewScrawlingItem
from datetime import datetime, timedelta
import re

# 시사뉴스만
class kidNewsSpiderCurrentAffairs(scrapy.Spider):
    name = "kidNewsSpiderCurrentAffairs"
    page = 1
    articles = 0
    # stop_date = '2020-08-31'
    # stop_date = datetime.strptime(stop_date, "%Y-%m-%d").date()
    stop_date = (datetime.now() - timedelta(1)).date()
    
    
    def start_requests(self):
        while kidNewsSpiderCurrentAffairs.page:
            url = 'http://kid.chosun.com/list_kj.html?catid=111&pn={page}'.format(page=kidNewsSpiderCurrentAffairs.page)

            yield scrapy.Request(url=url, callback=self.parse_url)
        
    def parse_url(self, response):
        # 인코딩 변환
        if response.encoding == 'cp1252':
            response = response.replace(encoding='euc-kr')
        
        default_url = 'http://kid.chosun.com'
        urls = list(map(lambda x: default_url + x, response.xpath('//div[@class="subject"]/a/@href').extract()))
        
        for url in urls:
            yield response.follow(url=url, callback = self.parse_article)
        kidNewsSpiderCurrentAffairs.page += 1
            
    def parse_article(self, response):
        item = KidNewScrawlingItem()
        
        # 기사 url
        item["news_url"] = response.url
        
        # 기사 제목
        item["news_title"] = response.xpath('//title/text()').extract_first()

        # 기사 부제목, 없는 경우도 존재
        subtitle = response.xpath('//h3/text()').extract_first()
        if subtitle:
            item["news_subtitle"] = subtitle
        else:
            item["news_subtitle"] = ""

        # 기자, 기자가 아닌 경우도 존재
        author = response.xpath('//span[@class="author"]/text()').extract_first()
        if author:
            item["news_writer"] = re.sub(r'[\r\n\t정리=]', '', author).strip()
        else:
            item["news_writer"] = ""

        # 입력날짜
        date = response.xpath('//span[@class="date"]/text()').extract_first()
        date = re.sub(r'[\r\n\t]', '', date).strip()[5:21]
        date = re.sub(r'[\.]', '-', date)
        # item["news_date"] = date
        item["news_date"] = datetime.strptime(date, '%Y-%m-%d %H:%M')
        
        # 본문
        paragraphs = response.xpath('//div[@class="Paragraph"]//text()[normalize-space() \
                                                    and not(ancestor::*/@class="center_img") \
                                                    and not(ancestor::*/@class="right_img")]').extract()
        p = " ".join([re.sub(r'[\r\n\t<사진>\xa0]', '', paragraph).strip() for paragraph in paragraphs])
        item["news_article"] = p

        # 기사 첫 사진 이미지 url
        img_path = response.xpath('//img[contains(@id, "artImg")]/@src').extract_first()
        if img_path:
            item["news_img"] = img_path
        else:
            item["news_img"] = ""
        
        # 신문지
        item["news_source"] = "어린이조선일보"

        # date가 stop_date와 같으면 pass 및 loop 종료 위해 0
        # if datetime.strptime(date, "%Y-%m-%d %H:%M").date() <= kidNewsSpiderCurrentAffairs.stop_date:
        if item["news_date"].date() <= kidNewsSpiderCurrentAffairs.stop_date:
            kidNewsSpiderCurrentAffairs.page = 0
            return

        # 본문이 존재하지 않을 경우 pass
        if not p:
            return

        kidNewsSpiderCurrentAffairs.articles += 1
        print(kidNewsSpiderCurrentAffairs.articles, "articles crawled")

        yield item