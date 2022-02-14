import scrapy
from scrapy.item import Field
from itemloaders.processors import TakeFirst, MapCompose, Join
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from scrapy.http import JsonRequest
import pandas as pd


headers = { 'authority': 'www.tripadvisor.co.uk',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'sec-ch-ua-mobile': '?0',
    'x-requested-by': 'TNI1625!AG1YRRpHOjQMgbfsrg1FWY4Ai8UH+StE3D7tD1/oCg3qzWRAYM2ff14YfUM2JUbFAl0x6vTP5McIcIHK3vGsWp/OUNzOT5pIGiZKb0BGLlQkrHttvrrkMiEX1B08Oy4WjTHFseLIh9VcHJi4Gh0/+LjAQFKarv7VPh3A6Lba2SV/',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 Safari/537.36',
    'sec-ch-ua-platform': '"macOS"',
    'accept': '*/*',
    'origin': 'https://www.tripadvisor.co.uk',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    #'referer': 'https://www.tripadvisor.co.uk/Cruises-g4-Europe-Cruises',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
}

class CruisesItem(scrapy.Item):
    ID = Field(output_processor = TakeFirst())
    cabinType = Field(output_processor = TakeFirst())
    packageType = Field(output_processor = TakeFirst())
    price = Field(output_processor = TakeFirst())
    vendor = Field(output_processor = TakeFirst())
    page_no = Field(output_processor = TakeFirst())


class CruisesSpider(scrapy.Spider):
    name = 'cruises_price'
    start_urls = ['https://www.tripadvisor.co.uk/data/graphql/ids']
    custom_settings = {
        'DOWNLOAD_DELAY':0.3,
        #'LOG_LEVEL':'INFO'
    }

    def start_requests(self):
        for urls in self.start_urls:
            for i in range(1, 600):
                yield JsonRequest(
                    url = urls, 
                    method = 'POST',
                    callback = self.parse,
                    headers = headers,
                    data = [
                            {
                                'query': '013d760a68c9a4f77e9a9a903e241eb8',
                                'variables': {
                                    'page': i,
                                    'limit': 20,
                                    'minPrice': None,
                                    'maxPrice': None,
                                    'order': 'popularity',
                                    'itineraryId': None,
                                    'vendorId': None,
                                    'cruiseLineId': None,
                                    'shipId': None,
                                    'cabinType': None,
                                    'departureDate': None,
                                    'length': None,
                                    'destinationId': [],
                                    'departurePortId': None,
                                    'portId': None,
                                    'cruiseStyleId': None,
                                    'dealId': None,
                                    'viewport': 'small',
                                    'locale': 'en_UK',
                                    'currency': 'GBP',
                                },
                            },
                        ],

            )
    def parse(self, response):
        container = response.json()

        for results in container:
            
            for data_results in results['data']['cruiseList']['pricing']:
                for price in data_results['meta']['results']:
                    for cabins in price['prices']['results']:
                        loader = ItemLoader(CruisesItem())
                        loader.add_value('ID', data_results['id'])
                        loader.add_value('cabinType', cabins['cabinType']['name'])
                        loader.add_value('packageType', cabins['packageType'])
                        loader.add_value('price', cabins['price'])
                        loader.add_value('vendor', price['vendor']['name'])
                        yield loader.load_item()

    # def parse_data(self, response):


process = CrawlerProcess(
    settings = {
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 Safari/537.36',
        'FEEDS':{
        'cruises_prices.jl':{
            'format':'jsonlines'
        }
    }
    }
)
process.crawl(CruisesSpider)
process.start()

data = pd.read_json('cruises_prices.jl', lines=True)
cruises_prices = data.pivot_table(index = ['ID', 'packageType', 'vendor'], columns='cabinType', values='price').reset_index()
cruises_prices.to_csv("cruise_prices.csv")