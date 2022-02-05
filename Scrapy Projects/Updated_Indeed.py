import scrapy
from pandas._libs.internals import defaultdict
from scrapy import Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join
import pandas as pd
from scrapy.crawler import CrawlerProcess

class IndeedItem(scrapy.Item):
    job_title = Field(output_processor=TakeFirst())
    salary = Field(output_processor=TakeFirst())
    category = Field(output_processor=TakeFirst())
    company = Field(output_processor=TakeFirst())
    post = Field(input_processor = MapCompose, 
    output_processor = Join())


class IndeedSpider(scrapy.Spider):
    name = 'indeed'

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
        #'DOWNLOAD_DELAY': 3
    }

    max_results_per_city = 1000
    names = pd.read_csv("indeed_names.csv")
    degree = pd.read_csv("indeed_names_test.csv")
    #names = pd.DataFrame({'names': ['London', 'Manchester']})
    #degree = pd.DataFrame({'degrees': ['degree+Finance+£25,000', 'degree+Engineering+£25,000'], 'degree_type': ['Finance', 'Engineering']})

    start_urls = defaultdict(list)

    def start_requests(self):
        for city in self.names.names:
            for degrees, degrees_entry,degrees_graduate, graduate_entry, graduate,sector  in zip(self.degree.degrees,self.degree.degrees_entry,self.degree.degrees_graduate,self.degree.graduate_entry,self.degree.graduate, self.degree.Sector):
                self.start_urls[sector].append(f'https://uk.indeed.com/jobs?q={degrees}&l={city}&fromage=7&filter=0&limit=100')
                self.start_urls[sector].append(f'https://uk.indeed.com/jobs?q={degrees_entry}&l={city}&fromage=7&filter=0&limit=100')
                self.start_urls[sector].append(f'https://uk.indeed.com/jobs?q={degrees_graduate}&l={city}&fromage=7&filter=0&limit=100')
                self.start_urls[sector].append(f'https://uk.indeed.com/jobs?q={graduate_entry}&l={city}&fromage=7&filter=0&limit=100')
                self.start_urls[sector].append(f'https://uk.indeed.com/jobs?q={graduate}&l={city}&fromage=7&filter=0&limit=100')
        for category, url in self.start_urls.items():
            for link in url:
                yield scrapy.Request(
                    link,
                    callback=self.parse,
                    #meta={'handle_httpstatus_list': [301]},
                    cb_kwargs={
                        'category': category
                    }
                )

    def parse(self, response, category):
        indeed = response.xpath('//div[@class="slider_container"]')
        for jobs in indeed:
            loader = ItemLoader(IndeedItem(), selector=jobs)
            loader.add_value('category', category)
            loader.add_xpath('job_title', './/span[@title]//text()')
            loader.add_xpath('salary', './/div[@class="salary-snippet"]/span//text()')
            loader.add_xpath('company', './/span[@class="companyName"]//text()')
            links = jobs.xpath('//a[@data-mobtk="1fr0ol27ot61q801"]/@href').get()
            yield response.follow(
                response.urljoin(links, 
                callback = self.parse_posts),
                cb_kwargs = {
                    'loader':loader
                }
            )

        next_page = response.xpath('//ul[@class="pagination-list"]//li[last()]/a/@href').get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
                cb_kwargs={
                    'category': category
                }
            )
    def parse_posts(self, response, loader):
        loader.add_value('posts',response.xpath("//div[@id='jobDescriptionText']//p//text()").getall())
        yield loader.load_item()

process = CrawlerProcess(
    settings = {
        'FEED_URI': 'indeed_posts.jl',
        'FEED_FORMAT':'jsonlines'
    }

)
process.crawl(IndeedSpider)
process.start()
