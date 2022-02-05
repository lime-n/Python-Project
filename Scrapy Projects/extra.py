import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess
#from scrapy_playwright.page import PageCoroutine
cookies = {
    'SID': 'EwgjPOmG2PWuaBTlR9NEO-8UX6QamVBfRw_WZejfxzCFWgYlrECgcncAg-kMu6xPsvZIvA.',
    '__Secure-1PSID': 'EwgjPOmG2PWuaBTlR9NEO-8UX6QamVBfRw_WZejfxzCFWgYl95Ka7oUk42PBmwTIeYSHjA.',
    '__Secure-3PSID': 'EwgjPOmG2PWuaBTlR9NEO-8UX6QamVBfRw_WZejfxzCFWgYlsECWg_Cx6x9QCg22fh5teg.',
    'HSID': 'ACwPCj50aumyorwWb',
    'SSID': 'AS9revwjYMK7QX9PN',
    'APISID': 'tLQA13bXMFC737Rh/A1NZAMpwFRyEZ3jUy',
    'SAPISID': 'IS0q2uQ0Tg1Nteao/AHUS_DRYSNuyXFsIE',
    '__Secure-1PAPISID': 'IS0q2uQ0Tg1Nteao/AHUS_DRYSNuyXFsIE',
    '__Secure-3PAPISID': 'IS0q2uQ0Tg1Nteao/AHUS_DRYSNuyXFsIE',
    'OGPC': '19022519-1:',
    'NID': '511=cE3rZrGpBkSe0jS2LYo7pcAPqHdEVqUJgVQ3U37DnOC_BycBKl0R177OINAXI8lQNWCzncGZUuPfHgxRHKQyQH00aw93k8D3eBLkj18mpjpTq8auR4G8iXLNIfbHkJcaP6eNDgF7-HrA-NUg-_mbxR_kVbB0OqzRDPKMkYKLpBWwx7xzhy8GxVvg5Dzzs0yQXUIYYEmkfc1ruSmyTf4vxL06frS9Dr4snKNwLT8XYV0lCGDTFpoZ2VNsxkwQ3iuNwgYGuWWYI49VzMJg8Za8k_V4t1MRbJ1NdSZzCizBviro19u0qEAY-gF1mB_6Jkvp',
    'DV': 'I6BxIAixTWQQgIjjJVKBVqah0kka5Rc',
}

headers = {
    'authority': 'www.google.co.uk',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
    'sec-ch-ua-platform': '"macOS"',
    'accept': '*/*',
    'x-client-data': 'CKe1yQEIjbbJAQimtskBCMS2yQEIqZ3KAQi/4MoBCOvyywEInvnLAQjW/MsBCOeEzAEIvovMAQisjswBCNKPzAEI2ZDMAQiRlcwBGKqpygEYjp7LAQ==',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://www.google.co.uk/',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
}
class ExtraSpider(scrapy.Spider):
    name = 'extra'
    districts = pd.read_csv("/Users/emiljanmrizaj/Downloads/Asset_Sustainable_City_France.csv")
    start_urls = []
    for places in districts.Districts.dropna():
        start_urls.append(f'https://www.google.co.uk/search?q={places}+france')

    custom_settings = {
        'User_Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
        'DOWNLOAD_DELAY':4
    }

    def start_reqests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url = url, 
                headers=headers,
                cookies=cookies,
                callback=self.parse )

    def parse(self, response):
        container = response.xpath("//div[@class='I6TXqe']")
        for titles in container:
            yield {
                'title':titles.xpath(".//h2[@class='qrShPb kno-ecr-pt PZPZlf q8U8x']//text()"),
                'category':titles.xpath(".//div[@class='wwUB2c PZPZlf E75vKf']//text()")

            }

process = CrawlerProcess(
    settings={
            "CONCURRENT_REQUESTS": 32,
            "FEED_URI":'jobs_test2.jl',
            "FEED_FORMAT":'jsonlines',
        }
    )
process.crawl(ExtraSpider)
process.start()