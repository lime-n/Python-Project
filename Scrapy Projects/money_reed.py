from scrapy.item import Field
import scrapy
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from itemloaders.processors import TakeFirst, MapCompose, Join
import pandas as pd
from collections import defaultdict
from scrapy_splash import SplashRequest

headers = {
    'authority': 'api2.branch.io',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
    'sec-ch-ua-platform': '"macOS"',
    'content-type': 'application/x-www-form-urlencoded',
    'accept': '*/*',
    'origin': 'https://www.reed.co.uk',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://www.reed.co.uk/',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
}

class ReedItem(scrapy.Item):
    category = Field(output_processor = TakeFirst())
    salary = Field(output_processor = TakeFirst())
    title =  Field(output_processor = TakeFirst())
    organisation = Field(output_processor = TakeFirst())
    region = Field(input_processor = MapCompose(str.strip),
        output_processor = TakeFirst())
    items = Field(output_processor = TakeFirst())
    post = Field(input_processor = MapCompose(),
    output_processor = Join(" "))

class ReedSpider(scrapy.Spider):
    name = 'reed'
    degree = pd.read_csv('/Users/emiljanmrizaj/Scrapy_Tickets/Items/indeed/indeed/degree_names2.csv')
    start_urls = defaultdict(list)

    custom_settings = {
        'USER_AGENT':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
        'DOWNLOAD_DELAY':0.5,
        #'LOG_LEVEL':'INFO',
        
    }

    def start_requests(self):
        for degrees, degrees_entry,degrees_graduate, graduate_entry, graduate,sector  in zip(self.degree.degrees,self.degree.degrees_entry,self.degree.degrees_graduate,self.degree.graduate_entry,self.degree.graduate, self.degree.sector):
            self.start_urls[sector].append(f'https://www.reed.co.uk/jobs/{degrees}-jobs')
            self.start_urls[sector].append(f'https://www.reed.co.uk/jobs/{degrees_entry}-jobs')
            self.start_urls[sector].append(f'https://www.reed.co.uk/jobs/{graduate_entry}-jobs')
            self.start_urls[sector].append(f'https://www.reed.co.uk/jobs/{degrees_graduate}-jobs')
            self.start_urls[sector].append(f'https://www.reed.co.uk/jobs/{graduate}-jobs')

        for items, urls in self.start_urls.items():
            for url in urls:
                yield scrapy.Request(
                    url = url, 
                    headers=headers,
                    callback = self.parse,
                    cb_kwargs = {
                        'items':items
                    }
            )
    def parse(self, response, items):
        container = response.xpath("//div[@class='row search-results']")
        for lists in container:
            loader = ItemLoader(ReedItem(), selector = lists)
            loader.add_value('items', items)
            loader.add_xpath('region', "(//div[@class='metadata']//ul)[position() mod 2=0]//li//text()")
            loader.add_xpath('category', "//div[@class='col-sm-11 col-xs-12 page-title']//h1/text()")
            loader.add_xpath('title', '//h3[@class="title"]/a/@title')
            loader.add_xpath('salary', '//li[@class="salary"]/text()')
            loader.add_xpath('organisation', '//a[@class="gtmJobListingPostedBy"]/text()')
            links = response.xpath('//h3[@class="title"]/a/@href').get()
            yield response.follow(
                response.urljoin(links),
                callback = self.parse_jobs,
                cb_kwargs = {
                    'loader':loader
                }
                
            )
        
        next_page = lists.xpath('//a[@id="nextPage"]/@href').get()
        if next_page:
            yield response.follow(
                    url = next_page,
                    callback = self.parse, 
                    headers=headers,
                    cb_kwargs = {
                        'items':items
                    }
                )
    def parse_jobs(self, response, loader):
        loader.add_value('post',response.xpath('(//span[@itemprop="description"]/p/text()) | (//span[@itemprop="description"]/p//text())  | (//span[@itemprop="description"]/ul//li/text())').getall())
        yield loader.load_item()

process = CrawlerProcess(
    settings = {
        'FEED_URI':'reed_jobs_post.jl',
        'FEED_FORMAT':'jsonlines'
    }
)
process.crawl(ReedSpider)
process.start()