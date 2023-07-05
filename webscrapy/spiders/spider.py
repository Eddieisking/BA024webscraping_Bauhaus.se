"""
Project: Web scraping for customer reviews
Author: Hào Cui
Date: 07/04/2023
"""
import json
import re

import scrapy
from scrapy import Request

from webscrapy.items import WebscrapyItem


class SpiderSpider(scrapy.Spider):
    name = "spider"
    allowed_domains = ["www.bauhaus.se", "staticw2.yotpo.com"]
    headers = {
        # 'app_key': 'Q8o34TFsHR6KLWxFbCCuNnMd1090IusN6buIuzr0',
        # 'Accept': 'application/json',
    }  #

    def start_requests(self):
        # keywords = ['DeWalt', 'Black+and+Decker', 'Stanley', 'Craftsman', 'Porter-Cable', 'Bostitch', 'Irwin+Tools',
        #             'Lenox']
        # company = 'Stanley Black and Decker'

        keywords = ['dewalt']
        # from search words to generate product_urls
        for keyword in keywords:
            push_key = {'keyword': keyword}
            search_url = f'https://www.bauhaus.se/varumarken/{keyword}'

            yield Request(
                url=search_url,
                callback=self.parse,
                cb_kwargs=push_key,
            )

    def parse(self, response, **kwargs):

        # Extract the pages of product_urls
        page = response.xpath('//*[@id="html-body"]/div[@class="toolbar-maincontent-wrapper page-main"]//div['
                              '@class="category-products-toolbar__amount"]/text()')[0].extract()
        page_number = int(''.join(filter(str.isdigit, page)))
        pages = (page_number // 35) + 1

        # Based on pages to build product_urls
        keyword = kwargs['keyword']
        product_urls = [f'https://www.bauhaus.se/varumarken/{keyword}?p={page}' for page
                        in range(1, pages + 1)]

        for product_url in product_urls:
            yield Request(url=product_url, callback=self.product_parse)

    def product_parse(self, response: Request, **kwargs):

        product_list = response.xpath('//*[@id="layer-product-list"]/div[@class="grid products-grid products '
                                      'wrapper"]/ol/li')

        for product in product_list:
            product_href = product.xpath('.//a[@class="card"]/@href')[0].extract()
            product_id = product.xpath('./a//div[@class="card__details-sub"]/div/@data-product-id')[0].extract()
            push_key = {'product_id': product_id}
            product_detailed_url = product_href
            yield Request(url=product_detailed_url, callback=self.product_detailed_parse, cb_kwargs=push_key)

    def product_detailed_parse(self, response, **kwargs):
        product_id = kwargs['product_id']
        customer_review_url = f'https://staticw2.yotpo.com/batch/app_key/Q8o34TFsHR6KLWxFbCCuNnMd1090IusN6buIuzr0' \
                              f'/domain_key/{product_id}/widget/main_widget '

        headers = {
            'Content-Type': 'application/json',
        }

        payload = {
            "methods": [
                {
                    "method": "main_widget",
                    # "method": "reviews",

                    "params": {
                        "pid": product_id,
                        "order_metadata_fields": {},
                        "widget_product_id": product_id,
                    }
                }
            ],
            "app_key": "Q8o34TFsHR6KLWxFbCCuNnMd1090IusN6buIuzr0",
            "is_mobile": False,
            "widget_version": "2023-07-05_08-43-33"
        }

        yield scrapy.Request(url=customer_review_url, method='POST', headers=headers, body=json.dumps(payload),
                             callback=self.review_parse,)

    def review_parse(self, response: Request, **kwargs):
        datas = json.loads(response.body)

        method = datas[0]["method"]
        result = datas[0]["result"]
        widget_product_id = datas[0]["widget_product_id"]

        selector = scrapy.Selector(text=result)
        # Total numbers of reviews, each page has 10 reviews
        total_reviews = selector.xpath('//span[@class="font-color-gray based-on"]/text()').extract_first()
        if total_reviews:
            total_number = int(re.findall(r'\d+', total_reviews)[0])
            pages = (total_number // 10) + 1
        else:
            pages = 0

        # Extract the reviews from the result
        review_list = selector.xpath(
            '//div[@class="yotpo-nav-content"]//div[@class="yotpo-review yotpo-regular-box  "]')

        for review in review_list:
            item = WebscrapyItem()

            item['review_id'] = review.xpath('./@data-review-id')[0].extract()
            item['customer_name'] = review.xpath('.//div[@class="yotpo-header-element "]/span/text()')[0].extract()
            item['customer_rating'] = float(
                review.xpath('.//div[@class="yotpo-review-stars "]/span[@class="sr-only"]/text()')[0].extract().split()[0])
            item['customer_date'] = review.xpath('.//span[@class="y-label yotpo-review-date"]/text()')[0].extract()
            item['customer_review'] = review.xpath('.//div[@class="content-review"]/text()')[0].extract()
            item['product_name'] = review.xpath(
                './/a[@class="product-link-wrapper "]/div[@class="y-label product-link"]/text()')[0].extract()
            item['customer_support'] = review.xpath(
                './/div[@class="yotpo-footer "]/div[@class="yotpo-helpful"]/span[@data-type="up"]/text()')[0].extract()
            item['customer_disagree'] = review.xpath(
                './/div[@class="yotpo-footer "]/div[@class="yotpo-helpful"]/span[@data-type="down"]/text()')[0].extract()

            yield item

        if pages > 1:
            for i in range(2, pages + 1):
                customer_review_url_more = f'https://staticw2.yotpo.com/batch/app_key/Q8o34TFsHR6KLWxFbCCuNnMd1090IusN6buIuzr0/domain_key/{widget_product_id}/widget/reviews'

                headers = {
                    'Content-Type': 'application/json',
                }
                payload = {
                    "methods": [
                        {
                            "method": "reviews",
                            "params": {
                                "pid": widget_product_id,
                                "order_metadata_fields": {},
                                "widget_product_id": widget_product_id,
                                "data_source": "default",
                                "page": i,
                                "host-widget": "main_widget",
                                "is_mobile": False,
                                "pictures_per_review": 10
                            }
                        }
                    ],
                    "app_key": "Q8o34TFsHR6KLWxFbCCuNnMd1090IusN6buIuzr0",
                    "is_mobile": False,
                    "widget_version": "2023-07-05_08-43-33"
                }

                yield scrapy.Request(url=customer_review_url_more, method='POST', headers=headers, body=json.dumps(payload),
                                     callback=self.review_parse_more)

    def review_parse_more(self, response: Request, **kwargs):
        datas = json.loads(response.body)

        method = datas[0]["method"]
        result = datas[0]["result"]
        widget_product_id = datas[0]["widget_product_id"]
        selector = scrapy.Selector(text=result)

        # Extract the reviews from the result
        review_list = selector.xpath(
            '//div[@class="yotpo-review yotpo-regular-box  "]')

        for review in review_list:
            item = WebscrapyItem()

            item['review_id'] = review.xpath('./@data-review-id')[0].extract()
            item['customer_name'] = review.xpath('.//div[@class="yotpo-header-element "]/span/text()')[0].extract()
            item['customer_rating'] = float(
                review.xpath('.//div[@class="yotpo-review-stars "]/span[@class="sr-only"]/text()')[0].extract().split()[0])
            item['customer_date'] = review.xpath('.//span[@class="y-label yotpo-review-date"]/text()')[0].extract()
            item['customer_review'] = review.xpath('.//div[@class="content-review"]/text()')[0].extract()
            item['product_name'] = review.xpath(
                './/a[@class="product-link-wrapper "]/div[@class="y-label product-link"]/text()')[0].extract()
            item['customer_support'] = review.xpath(
                './/div[@class="yotpo-footer "]/div[@class="yotpo-helpful"]/span[@data-type="up"]/text()')[0].extract()
            item['customer_disagree'] = review.xpath(
                './/div[@class="yotpo-footer "]/div[@class="yotpo-helpful"]/span[@data-type="down"]/text()')[0].extract()

            yield item



