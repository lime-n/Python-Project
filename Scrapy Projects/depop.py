import scrapy
from scrapy.crawler import CrawlerProcess

class DepopSpider(scrapy.Spider):
    name = 'depop'
    start_urls = ['https://webapi.depop.com/api/v2/search/filters/aggregates/?brands=1596&itemsPerPage=24&country=gb&currency=GBP&sort=relevance']

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url, 
                callback = self.parse
            )

    def parse(self, response):
        brands = response.json().get('brands')
        for num,br in zip(brands,brands.values()):
            yield {
            'brand_code':br,
            'brand_num':num
        }

process = CrawlerProcess(
    settings = {
        'FEED_URI':'depop.jl',
        'FEED_FORMAT':'jsonlines'
    }
)

process.crawl(DepopSpider)
process.start()