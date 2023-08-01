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
        keywords = ['dewalt', 'Stanley', 'Black+Decker', 'Craftsman', 'Porter-Cable', 'Bostitch', 'Facom', 'Proto', 'MAC Tools', 'Vidmar', 'Lista', 'Irwin', 'Lenox', 'CribMaster', 'Powers Fasteners', 'cub-cadet', 'hustler', 'troy-bilt', 'BigDog Mower',]
        exist_keywords_standard = ['dewalt', 'stanley']
        """exist_keywords_abnormal needs to be do in another project"""
        exist_keywords_abnormal = ['Black+Decker', 'Irwin',]
        # company = 'Stanley Black and Decker'
        # from search words to generate product_urls
        for keyword in exist_keywords_standard:
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
                        in range(1, pages + 1)]  #pages + 1

        for product_url in product_urls:
            yield Request(url=product_url, callback=self.product_parse, meta={'product_brand': keyword})

    def product_parse(self, response: Request, **kwargs):
        product_brand = response.meta['product_brand']
        product_list = response.xpath('//*[@id="layer-product-list"]/div[@class="grid products-grid products '
                                      'wrapper"]/ol/li')

        for product in product_list:
            product_href = product.xpath('.//a[@class="card"]/@href')[0].extract()
            product_id = product.xpath('./a//div[@class="card__details-sub"]/div/@data-product-id')[0].extract()
            push_key = {'product_id': product_id}
            product_detailed_url = product_href
            yield Request(url=product_detailed_url, callback=self.product_detailed_parse, cb_kwargs=push_key, meta={'product_brand': product_brand})

    def product_detailed_parse(self, response, **kwargs):
        product_brand = response.meta['product_brand']
        product_id = kwargs['product_id']
        product_detail = response.xpath('//tbody/tr')
        product_model = 'N/A'
        product_type = 'N/A'

        for product in product_detail:
            attr = product.xpath('./th/text()')[0].extract()
            value = product.xpath('./td/text()')[0].extract()

            if attr == "Varumärke":
                product_brand = value if value else 'N/A'
            elif attr == 'Artikelnummer':
                product_model = value if value else 'N/A'
            elif attr == 'Typ':
                product_type = value if value else 'N/A'

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
                             callback=self.review_parse, meta={'product_model':product_model, 'product_brand':product_brand, 'product_type':product_type})

    def review_parse(self, response: Request, **kwargs):
        product_brand = response.meta['product_brand']
        product_model = response.meta['product_model']
        product_type = response.meta['product_type']

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
            item['product_website'] = 'bauhaus_se'
            item['product_type'] = product_type
            item['product_brand'] = product_brand
            item['product_model'] = product_model
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
                                     callback=self.review_parse_more, meta={'product_model':product_model, 'product_brand':product_brand, 'product_type':product_type})

    def review_parse_more(self, response: Request, **kwargs):
        product_brand = response.meta['product_brand']
        product_model = response.meta['product_model']
        product_type = response.meta['product_type']
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
            item['product_website'] = 'bauhaus_se'
            item['product_type'] = product_type
            item['product_brand'] = product_brand
            item['product_model'] = product_model
            item['customer_support'] = review.xpath(
                './/div[@class="yotpo-footer "]/div[@class="yotpo-helpful"]/span[@data-type="up"]/text()')[0].extract()
            item['customer_disagree'] = review.xpath(
                './/div[@class="yotpo-footer "]/div[@class="yotpo-helpful"]/span[@data-type="down"]/text()')[0].extract()

            yield item


# """This spider is for exist_keywords_abnormal"""
# class SpiderSpider(scrapy.Spider):
#     name = "spider"
#     allowed_domains = ["www.bauhaus.se", "staticw2.yotpo.com", "28wf35c8yr-dsn.algolia.net"]
#     headers = {
#         # 'app_key': 'Q8o34TFsHR6KLWxFbCCuNnMd1090IusN6buIuzr0',
#         # 'Accept': 'application/json',
#     }  #
#
#     def start_requests(self):
#         keywords = ['dewalt', 'Stanley', 'Black+Decker', 'Craftsman', 'Porter-Cable', 'Bostitch', 'Facom', 'Proto', 'MAC Tools', 'Vidmar', 'Lista', 'Irwin', 'Lenox', 'CribMaster', 'Powers Fasteners', 'cub-cadet', 'hustler', 'troy-bilt', 'BigDog Mower',]
#         exist_keywords_standard = ['dewalt', 'stanley']
#
#         """exist_keywords_abnormal needs to be do in another project"""
#         exist_keywords_abnormal = ['Black+Decker', 'Irwin',]
#         for keyword in exist_keywords_abnormal:
#             push_key = {'keyword': keyword}
#             search_url = f'https://28wf35c8yr-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.13.1)%3B%20Browser%3B%20instantsearch.js%20(4.41.0)%3B%20Magento2%20integration%20(3.8.1)%3B%20JS%20Helper%20(3.8.2)'
#
#             headers = {
#                 'Accept': '*/*',
#                 'Accept-Encoding': 'gzip, deflate, br',
#                 'Accept-Language': 'zh-CN,zh;q=0.9,en-GB;q=0.8,en;q=0.7',
#                 'Connection': 'keep-alive',
#                 'Content-Type': 'application/x-www-form-urlencoded',
#                 'Host': '28wf35c8yr-dsn.algolia.net',
#                 'Origin': 'https://www.bauhaus.se',
#                 'Referer': 'https://www.bauhaus.se/',
#                 'Sec-Ch-Ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
#                 'Sec-Ch-Ua-Mobile': '?0',
#                 'Sec-Ch-Ua-Platform': '"macOS"',
#                 'Sec-Fetch-Dest': 'empty',
#                 'Sec-Fetch-Mode': 'cors',
#                 'Sec-Fetch-Site': 'cross-site',
#                 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
#                 'X-Algolia-Api-Key': 'NGE0NDE3NDk4ODY0NThlODAzMDdlMjY1MWFmYmVhNjk2OTJkYWM3MTBmNjc0MjJlNmE4YTM4MGU1MDNmOWUxZWF0dHJpYnV0ZXNUb1JldHJpZXZlPSU1QiUyMm5hbWUlMjIlMkMlMjJza3UlMjIlMkMlMjJtYW51ZmFjdHVyZXIlMjIlMkMlMjJjYXRlZ29yaWVzJTIyJTJDJTIyY29sb3IlMjIlMkMlMjJyYXRpbmdfc3VtbWFyeSUyMiUyQyUyMmJyYW5kJTIyJTJDJTIyZW5lcmd5bXJrJTIyJTJDJTIyZGlyZGVseW4lMjIlMkMlMjJwcmljZV90eXBlJTIyJTJDJTIyYXJ0X2Fzc29ydG1lbnQlMjIlMkMlMjJhdmFpbGFiaWxpdHklMjIlMkMlMjJlYW4lMjIlMkMlMjJvcmRwcmlzJTIyJTJDJTIybWluaW1hbF9wcmljZSUyMiUyQyUyMmlmYWN0b3JfcHJpY2UlMjIlMkMlMjJpZmFjdG9yX29yZHByaXMlMjIlMkMlMjJpZmFjdG9yX3VuaXQlMjIlMkMlMjJwcm9kdWN0dHlwZSUyMiUyQyUyMmNhbXBhaWduX3ByaWNlX2Zyb21fZGF0ZSUyMiUyQyUyMmNhbXBhaWduX3ByaWNlX3RvX2RhdGUlMjIlMkMlMjJvbl9zYWxlJTIyJTJDJTIycGF0aCUyMiUyQyUyMm1ldGFfdGl0bGUlMjIlMkMlMjJtZXRhX2tleXdvcmRzJTIyJTJDJTIybWV0YV9kZXNjcmlwdGlvbiUyMiUyQyUyMnByb2R1Y3RfY291bnQlMjIlMkMlMjJvYmplY3RJRCUyMiUyQyUyMnVybCUyMiUyQyUyMnZpc2liaWxpdHlfc2VhcmNoJTIyJTJDJTIydmlzaWJpbGl0eV9jYXRhbG9nJTIyJTJDJTIyY2F0ZWdvcmllc193aXRob3V0X3BhdGglMjIlMkMlMjJ0aHVtYm5haWxfdXJsJTIyJTJDJTIyaW1hZ2VfdXJsJTIyJTJDJTIyaW1hZ2VzX2RhdGElMjIlMkMlMjJpbl9zdG9jayUyMiUyQyUyMnR5cGVfaWQlMjIlMkMlMjJ2YWx1ZSUyMiUyQyUyMnF1ZXJ5JTIyJTJDJTIyaXNfd2ViJTIyJTJDJTIyaXNfd2ViX25vdGUlMjIlMkMlMjJ3ZWJfbm90ZSUyMiUyQyUyMndlYl9pbl9zdG9ja190ZXh0JTIyJTJDJTIyd2ViX291dF9vZl9zdG9ja190ZXh0JTIyJTJDJTIyaXNfcGh5c2ljJTIyJTJDJTIyaXNfcGh5c2ljX25vdGUlMjIlMkMlMjJwaHlzaWNfbm90ZSUyMiUyQyUyMnBoeXNpY19pbl9zdG9ja190ZXh0JTIyJTJDJTIycGh5c2ljX291dF9vZl9zdG9ja190ZXh0JTIyJTJDJTIycGh5c2ljX2Nzc19jbGFzcyUyMiUyQyUyMndlYl9jc3NfY2xhc3MlMjIlMkMlMjJub19wcmljZSUyMiUyQyUyMm91dGdvaW5nX2Jhbm5lciUyMiUyQyUyMm5ld19iYW5uZXIlMjIlMkMlMjJzYWxlc19iYW5uZXIlMjIlMkMlMjJzZWVfcHJpY2VfYmFubmVyJTIyJTJDJTIyZW5lcmd5X2Fycm93JTIyJTJDJTIyZW5lcmd5X3BkZiUyMiUyQyUyMmVuZXJneV9pbmZvJTIyJTJDJTIyZG91YmxlX2RlbnNpdHlfaW1hZ2VfdXJsJTIyJTJDJTIyaW52ZW50b3J5JTIyJTJDJTIybG93ZXN0X3ByaWNlJTIyJTJDJTIycHJpY2UuU0VLLnR5cGUlMjIlMkMlMjJwcmljZS5TRUsuZ3JvdXBfMCUyMiUyQyUyMnByaWNlLlNFSy5ncm91cF8wX2RlZmF1bHRfZm9ybWF0dGVkJTIyJTJDJTIycHJpY2UuU0VLLmdyb3VwXzBfb3JkaW5hcnlfcHJpY2UlMjIlMkMlMjJwcmljZS5TRUsuZ3JvdXBfMF9vcmRpbmFyeV9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5TRUsuZ3JvdXBfMF91bml0X3ByaWNlJTIyJTJDJTIycHJpY2UuU0VLLmdyb3VwXzBfdW5pdF9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5TRUsuZ3JvdXBfMF91bml0X29yZGluYXJ5X3ByaWNlJTIyJTJDJTIycHJpY2UuU0VLLmdyb3VwXzBfdW5pdF9vcmRpbmFyeV9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5TRUsuZ3JvdXBfMF9zYXZlZF9hbW91bnRfZm9ybWF0dGVkJTIyJTJDJTIycHJpY2UuTk9LLnR5cGUlMjIlMkMlMjJwcmljZS5OT0suZ3JvdXBfMCUyMiUyQyUyMnByaWNlLk5PSy5ncm91cF8wX2RlZmF1bHRfZm9ybWF0dGVkJTIyJTJDJTIycHJpY2UuTk9LLmdyb3VwXzBfb3JkaW5hcnlfcHJpY2UlMjIlMkMlMjJwcmljZS5OT0suZ3JvdXBfMF9vcmRpbmFyeV9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5OT0suZ3JvdXBfMF91bml0X3ByaWNlJTIyJTJDJTIycHJpY2UuTk9LLmdyb3VwXzBfdW5pdF9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5OT0suZ3JvdXBfMF91bml0X29yZGluYXJ5X3ByaWNlJTIyJTJDJTIycHJpY2UuTk9LLmdyb3VwXzBfdW5pdF9vcmRpbmFyeV9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5OT0suZ3JvdXBfMF9zYXZlZF9hbW91bnRfZm9ybWF0dGVkJTIyJTJDJTIycHJpY2UuREtLLnR5cGUlMjIlMkMlMjJwcmljZS5ES0suZ3JvdXBfMCUyMiUyQyUyMnByaWNlLkRLSy5ncm91cF8wX2RlZmF1bHRfZm9ybWF0dGVkJTIyJTJDJTIycHJpY2UuREtLLmdyb3VwXzBfb3JkaW5hcnlfcHJpY2UlMjIlMkMlMjJwcmljZS5ES0suZ3JvdXBfMF9vcmRpbmFyeV9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5ES0suZ3JvdXBfMF91bml0X3ByaWNlJTIyJTJDJTIycHJpY2UuREtLLmdyb3VwXzBfdW5pdF9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5ES0suZ3JvdXBfMF91bml0X29yZGluYXJ5X3ByaWNlJTIyJTJDJTIycHJpY2UuREtLLmdyb3VwXzBfdW5pdF9vcmRpbmFyeV9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5ES0suZ3JvdXBfMF9zYXZlZF9hbW91bnRfZm9ybWF0dGVkJTIyJTJDJTIycHJpY2UuSVNLLnR5cGUlMjIlMkMlMjJwcmljZS5JU0suZ3JvdXBfMCUyMiUyQyUyMnByaWNlLklTSy5ncm91cF8wX2RlZmF1bHRfZm9ybWF0dGVkJTIyJTJDJTIycHJpY2UuSVNLLmdyb3VwXzBfb3JkaW5hcnlfcHJpY2UlMjIlMkMlMjJwcmljZS5JU0suZ3JvdXBfMF9vcmRpbmFyeV9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5JU0suZ3JvdXBfMF91bml0X3ByaWNlJTIyJTJDJTIycHJpY2UuSVNLLmdyb3VwXzBfdW5pdF9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5JU0suZ3JvdXBfMF91bml0X29yZGluYXJ5X3ByaWNlJTIyJTJDJTIycHJpY2UuSVNLLmdyb3VwXzBfdW5pdF9vcmRpbmFyeV9wcmljZV9mb3JtYXR0ZWQlMjIlMkMlMjJwcmljZS5JU0suZ3JvdXBfMF9zYXZlZF9hbW91bnRfZm9ybWF0dGVkJTIyJTVEJnRhZ0ZpbHRlcnM9',
#                 'X-Algolia-Application-Id': '28WF35C8YR'
#             }
#
#             #
#             # payload = {'requests': [{
#             #             'page': 2,
#             #             'ruleContexts': ["magento_filters"],
#             #             'hitsPerPage': 35,
#             #             'query': 'Black + Decker',
#             # }
#             #     ],
#             # "app_key": "Q8o34TFsHR6KLWxFbCCuNnMd1090IusN6buIuzr0",
#             #
#             # }
#
#             yield scrapy.Request(url=search_url, method='POST', headers=headers,
#                                  callback=self.review_parse, )
#             # yield Request(
#             #     url=search_url,
#             #     callback=self.parse,
#             #     cb_kwargs=push_key,
#             # )
#
#     def parse(self, response, **kwargs):
#         datas = json.loads(response.body)
#         print(datas)
#         # Extract the pages of product_urls
#         # page = response.xpath('//div[@class="algolia-instant-selector-results"]/div/h1/text()')[0].extract()
#         # '//*[@id="instant-title-results-container"]'
#         # '//div[@class="algolia-instant-selector-results"]/div/h1/text()'
#         # page_number = int(''.join(filter(str.isdigit, page)))
#         # page_number = 133
#         # pages = (page_number // 35) + 1
#         #
#         # # Based on pages to build product_urls
#         # keyword = kwargs['keyword']
#         # product_urls = [f'https://www.bauhaus.se/catalogsearch/result/?q={keyword}&page={page}' for page
#         #                 in range(1, pages + 1)]  #pages + 1
#         #
#         # for product_url in product_urls:
#         #     yield Request(url=product_url, callback=self.product_parse)
#
#     def product_parse(self, response: Request, **kwargs):
#         product_list = response.xpath('//*[@id="instant-search-results-container"]//ol[@class="ais-InfiniteHits-list"]/li')
#
#         for product in product_list:
#             product_href = product.xpath('.//a[@class="card"]/@href')[0].extract()
#             product_id = product.xpath('.//a[@class="card"]/@data-objectid')[0].extract()
#             push_key = {'product_id': product_id}
#             product_detailed_url = product_href
#             yield Request(url=product_detailed_url, callback=self.product_detailed_parse, cb_kwargs=push_key)
#
#     def product_detailed_parse(self, response, **kwargs):
#         product_id = kwargs['product_id']
#         customer_review_url = f'https://staticw2.yotpo.com/batch/app_key/Q8o34TFsHR6KLWxFbCCuNnMd1090IusN6buIuzr0' \
#                               f'/domain_key/{product_id}/widget/main_widget '
#
#         headers = {
#             'Content-Type': 'application/json',
#         }
#
#         payload = {
#             "methods": [
#                 {
#                     "method": "main_widget",
#                     # "method": "reviews",
#
#                     "params": {
#                         "pid": product_id,
#                         "order_metadata_fields": {},
#                         "widget_product_id": product_id,
#                     }
#                 }
#             ],
#             "app_key": "Q8o34TFsHR6KLWxFbCCuNnMd1090IusN6buIuzr0",
#             "is_mobile": False,
#             "widget_version": "2023-07-05_08-43-33"
#         }
#
#         yield scrapy.Request(url=customer_review_url, method='POST', headers=headers, body=json.dumps(payload),
#                              callback=self.review_parse,)
#
#     def review_parse(self, response: Request, **kwargs):
#         datas = json.loads(response.body)
#
#         method = datas[0]["method"]
#         result = datas[0]["result"]
#         widget_product_id = datas[0]["widget_product_id"]
#
#         selector = scrapy.Selector(text=result)
#         # Total numbers of reviews, each page has 10 reviews
#         total_reviews = selector.xpath('//span[@class="font-color-gray based-on"]/text()').extract_first()
#         if total_reviews:
#             total_number = int(re.findall(r'\d+', total_reviews)[0])
#             pages = (total_number // 10) + 1
#         else:
#             pages = 0
#
#         # Extract the reviews from the result
#         review_list = selector.xpath(
#             '//div[@class="yotpo-nav-content"]//div[@class="yotpo-review yotpo-regular-box  "]')
#
#         for review in review_list:
#             item = WebscrapyItem()
#
#             item['review_id'] = review.xpath('./@data-review-id')[0].extract()
#             item['customer_name'] = review.xpath('.//div[@class="yotpo-header-element "]/span/text()')[0].extract()
#             item['customer_rating'] = float(
#                 review.xpath('.//div[@class="yotpo-review-stars "]/span[@class="sr-only"]/text()')[0].extract().split()[0])
#             item['customer_date'] = review.xpath('.//span[@class="y-label yotpo-review-date"]/text()')[0].extract()
#             item['customer_review'] = review.xpath('.//div[@class="content-review"]/text()')[0].extract()
#             item['product_name'] = review.xpath(
#                 './/a[@class="product-link-wrapper "]/div[@class="y-label product-link"]/text()')[0].extract()
#             item['customer_support'] = review.xpath(
#                 './/div[@class="yotpo-footer "]/div[@class="yotpo-helpful"]/span[@data-type="up"]/text()')[0].extract()
#             item['customer_disagree'] = review.xpath(
#                 './/div[@class="yotpo-footer "]/div[@class="yotpo-helpful"]/span[@data-type="down"]/text()')[0].extract()
#
#             yield item
#
#         if pages > 1:
#             for i in range(2, pages + 1):
#                 customer_review_url_more = f'https://staticw2.yotpo.com/batch/app_key/Q8o34TFsHR6KLWxFbCCuNnMd1090IusN6buIuzr0/domain_key/{widget_product_id}/widget/reviews'
#
#                 headers = {
#                     'Content-Type': 'application/json',
#                 }
#                 payload = {
#                     "methods": [
#                         {
#                             "method": "reviews",
#                             "params": {
#                                 "pid": widget_product_id,
#                                 "order_metadata_fields": {},
#                                 "widget_product_id": widget_product_id,
#                                 "data_source": "default",
#                                 "page": i,
#                                 "host-widget": "main_widget",
#                                 "is_mobile": False,
#                                 "pictures_per_review": 10
#                             }
#                         }
#                     ],
#                     "app_key": "Q8o34TFsHR6KLWxFbCCuNnMd1090IusN6buIuzr0",
#                     "is_mobile": False,
#                     "widget_version": "2023-07-05_08-43-33"
#                 }
#
#                 yield scrapy.Request(url=customer_review_url_more, method='POST', headers=headers, body=json.dumps(payload),
#                                      callback=self.review_parse_more)
#
#     def review_parse_more(self, response: Request, **kwargs):
#         datas = json.loads(response.body)
#
#         method = datas[0]["method"]
#         result = datas[0]["result"]
#         widget_product_id = datas[0]["widget_product_id"]
#         selector = scrapy.Selector(text=result)
#
#         # Extract the reviews from the result
#         review_list = selector.xpath(
#             '//div[@class="yotpo-review yotpo-regular-box  "]')
#
#         for review in review_list:
#             item = WebscrapyItem()
#
#             item['review_id'] = review.xpath('./@data-review-id')[0].extract()
#             item['customer_name'] = review.xpath('.//div[@class="yotpo-header-element "]/span/text()')[0].extract()
#             item['customer_rating'] = float(
#                 review.xpath('.//div[@class="yotpo-review-stars "]/span[@class="sr-only"]/text()')[0].extract().split()[0])
#             item['customer_date'] = review.xpath('.//span[@class="y-label yotpo-review-date"]/text()')[0].extract()
#             item['customer_review'] = review.xpath('.//div[@class="content-review"]/text()')[0].extract()
#             item['product_name'] = review.xpath(
#                 './/a[@class="product-link-wrapper "]/div[@class="y-label product-link"]/text()')[0].extract()
#             item['customer_support'] = review.xpath(
#                 './/div[@class="yotpo-footer "]/div[@class="yotpo-helpful"]/span[@data-type="up"]/text()')[0].extract()
#             item['customer_disagree'] = review.xpath(
#                 './/div[@class="yotpo-footer "]/div[@class="yotpo-helpful"]/span[@data-type="down"]/text()')[0].extract()
#
#             yield item


