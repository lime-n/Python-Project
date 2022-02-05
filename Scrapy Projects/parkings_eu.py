from enum import unique
import scrapy
from scrapy.loader import ItemLoader
from scrapy.item import Field
from itemloaders.processors import TakeFirst, MapCompose, Join
from scrapy.crawler import CrawlerProcess
from bs4 import BeautifulSoup
import json
from price_parser import Price
import re
from itertools import zip_longest
import pandas as pd
from collections import defaultdict
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class DuplicatesPipeline:

    def __init__(self):
        self.titles_seen = set()

    def process_item(self, unique, spider):
        if unique['tref'] in self.titles_seen:
            raise DropItem("Duplicate item title found: %s" % unique)
        else:
            self.titles_seen.add(unique['tref'])
            return unique


data=pd.read_json("/Users/emiljanmrizaj/parking/parking/data.jl", lines=True)

from collections import defaultdict
filter_tags = defaultdict(list)
for keys in data.countries:
    filter_tags['tags'].append(keys.split(',')[0].split('(')[1])
    filter_tags['id'].append(keys.split(',')[1])
    filter_tags['name'].append(keys.split(',')[2].split(')')[0].lower())

filter_tags=pd.DataFrame(filter_tags)
filter_tags
filter_tags.tags=filter_tags.tags.astype('string')
filter_tags.id=filter_tags.id.astype('float')
filter_tags.name=filter_tags.name.astype('string')
#filter_tags.category=filter_tags.category.astype('string')
filter_tags.tags=filter_tags.tags.str.replace("'", '')
filter_tags.name=filter_tags.name.str.replace("'", '')

filter_data=pd.read_json("/Users/emiljanmrizaj/Scrapy_Tickets/ticket2/ticket2/json_data.jl", lines=True)
some_tags = defaultdict(list)
for kets, values in zip(filter_data.clicks, filter_data.category):
    some_tags['tags'].append(kets.split(',')[0].split('(')[1])
    some_tags['id'].append(kets.split(',')[1])
    some_tags['name'].append(kets.split(',')[2].split(')')[0].lower())
    some_tags['category'].append(values)

links = defaultdict(list)
for tags, id, name in zip(filter_tags.tags, filter_tags.id, filter_tags.name):
    links["name"].append(f'https://www.theparking.eu/used-cars/used-cars/#!/used-cars/%3F{tags}%3D{round(id)}')
    links["ID"].append(id)
    links['country'].append(name)

some_tags=pd.DataFrame(some_tags)
some_tags
some_tags.tags=some_tags.tags.astype('string')
some_tags.id=some_tags.id.astype('float')
some_tags.name=some_tags.name.astype('string')
some_tags.category=some_tags.category.astype('string')
some_tags.tags=some_tags.tags.str.replace("'", '')
some_tags.name=some_tags.name.str.replace("'", '')
import re
category_filters = defaultdict(list)
for i,j,k, z in zip(some_tags.tags, some_tags.category,some_tags.id, some_tags.name):
    if (i == 'id_marque')&(
         j == 'Make'):
         category_filters['id_marque'].append(i)
         category_filters['id_marque_make'].append(j)
         category_filters['id_marque_id'].append(k)
         category_filters['id_marque_name'].append(z)
    if (i == 'id_modele')&(
         j == 'Model'):
         category_filters['id_modele'].append(i)
         category_filters['id_modele_model'].append(j)
         category_filters['id_modele_id'].append(k)
         category_filters['id_modele_name'].append(z)
    if (i == 'id_motorisation')&(
         j == 'Engine'):
         category_filters['id_motorisation'].append(i)
         category_filters['id_motorisation_engine'].append(j)
         category_filters['id_motorisation_id'].append(k)
         category_filters['id_motorisation_name'].append(z)
    

def get_price(price_raw):
    price_object = Price.fromstring(price_raw)
    return price_object.amount_float

def get_currency(price_raw):
    price_object = Price.fromstring(price_raw)
    currency = price_object.currency
    return currency

headers = {
    'authority': 'www.theparking.eu',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
    'accept': '*/*',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'x-requested-with': 'XMLHttpRequest',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
    'sec-ch-ua-platform': '"macOS"',
    'origin': 'https://www.theparking.eu',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    #'referer': 'https://www.theparking.eu/used-cars/used-cars/',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
}

class Countryitem(scrapy.Item):
    make = Field(output_processor = TakeFirst())
    latest_price = Field(input_processor = MapCompose(get_price),
                        output_processor = TakeFirst())
    currency = Field(input_processor = MapCompose(get_currency),
                        output_processor = TakeFirst())
    old_price = Field(output_processor = TakeFirst())
    model = Field(output_processor = TakeFirst())
    sub_data = Field(output_processor = TakeFirst())
    mileage = Field(output_processor = TakeFirst())
    transmission= Field(output_processor = TakeFirst())
    year = Field(output_processor = TakeFirst())
    fuel = Field(output_processor = TakeFirst())
    text = Field(output_processor = TakeFirst())
    listing_date = Field(output_processor = TakeFirst())
    location = Field(output_processor = TakeFirst())
    postal_code = Field(output_processor = TakeFirst())
    country = Field(output_processor = TakeFirst())
    page_number = Field(output_processor = TakeFirst())
    unique_id = Field(output_processor = TakeFirst())

class CountrySpider(scrapy.Spider):
    name = "country"
    start_urls = ['https://www.theparking.eu/used-cars/#!/used-cars/%3Fid_pays%3D107']
    #for links, pages, id, country in zip(url_data.links, url_data.pages, url_data.id, url_data.country):

    custom_settings = {
        #'LOG_LEVEL':'INFO',
        'REACTOR_THREADPOOL_MAXSIZE':25,
        'CONCURRENT_REQUESTS_PER_DOMAIN':24,
        'CONCURRENT_REQUESTS':64,
        'COOKIES_ENABLED':False,
        'DOWNLOAD_TIMEOUT':15,
        #'DOWNLOAD_DELAY':5
    }

    def start_requests(self):
        for id_ in zip(links['ID']):
            for id_marque in category_filters['id_marque_id']:
                for models in category_filters['id_modele_id']:
                    for engine in category_filters['id_motorisation_id']:            
                        for page in range(1, 10000):
                            yield scrapy.FormRequest(
                                url = f'https://www.theparking.eu/used-cars/#!/used-cars/%3Fid_pays%3D{id_}%26id_marque%3D{id_marque}%26id_modele%3D{models}%26id_motorisation%3D{engine}',
                                method="POST",
                                callback = self.parse,
                                formdata =  {
                                    'ajax': '{"tab_id":"t0","cur_page":%s,"cur_trie":"distance","query":"","critere":{"id_pays":[%s],"id_marque":[%s], "id_modele":[%s], "id_motorisation":[%s]},"sliders":{"prix":{"id":"#range_prix","face":"prix","max_counter":983615,"min":"1","max":"400000"},"km":{"id":"#range_km","face":"km","max_counter":1071165,"min":"1","max":"500000"},"millesime":{"id":"#range_millesime","face":"millesime","max_counter":1163610,"min":"1900","max":"2022"}},"req_num":1,"nb_results":"11795660","current_location_distance":-1,"logged_in":false}' % (page,id_, id_marque, models, engine),
                                    'tabs': '["t0"]'
                                    },
                                headers=headers,

                                # cb_kwargs = {
                                #     'page_number':page
                                # }
                        )
    def parse(self, response):
        try:
            container = json.loads(response.text)
        except:
            pass
        test=container['#lists']
        soup = BeautifulSoup(test, 'lxml')
        #test2 = soup.select("a.external.tag_f_titre > span.sub-title.title-block:nth-child(3)")
        for i in soup:
            
            sub_data = i.select("a.external.tag_f_titre > span.sub-title.title-block:nth-child(3)")
            carMake = i.select("a.external.tag_f_titre > span.title-block.brand:nth-child(1)")
            carModel = i.select("a.external.tag_f_titre > span.sub-title.title-block:nth-child(2)")
            latest_price = i.select("ul.resultat section.clearfix div div > div.price-block > p.prix")
            old_price = i.select("ul.resultat section.clearfix div div > div.price-block > p.old-prix > s")
            carMileage = i.select("ul.info.clearfix li:nth-child(2) > div.upper")
            carTransmission = i.select("li:nth-child(4) > div.upper")
            carFuel = i.select("li:nth-child(1) > div.upper")
            carYear = i.select("li:nth-child(3) > div.upper")
            carListing = i.select("div.loc-date > p.btn-publication")
            carLocation = i.select("div.location > span.upper")
            carPostal = i.select("li:nth-child(5) > div.upper")
            carUnique = i.select('li[tref]')

            for make, newprice,model, subdata, mileage, oldprice, transmission, fuel, year, listing, location, postal, unique in zip_longest(
                carMake, latest_price, carModel, sub_data, carMileage, 
                old_price, carTransmission, carFuel, carYear, carListing, carLocation, carPostal,carUnique
                ):
                loader = ItemLoader(Countryitem())
                # loader.add_value('page_number', page_number)
                loader.add_value("unique_id", unique['tref'])

                if make != None:
                    loader.add_value('make', make.text)
                else:
                    loader.add_value('make', "None")
                if newprice != None:
                    loader.add_value('latest_price', re.sub(" ",'',re.sub(',','',newprice.text.strip())))

                else:
                    loader.add_value('latest_price', "None")
                    
                if newprice !=None:
                    loader.add_value('currency', re.sub(" ",'',re.sub(',','',newprice.text.strip())))
                else:
                    loader.add_value('currency', "None")
   
                if oldprice != None:
                    loader.add_value('old_price', oldprice.text)
                else:
                    loader.add_value('old_price', "None")
                if model != None:
                    loader.add_value('model', model.text)
                else:
                    loader.add_value('model', "None")
                if subdata != None:
                    loader.add_value('sub_data', subdata.get_text(" "))
                else:
                    loader.add_value('sub_data', "None")

                if mileage != None:
                    loader.add_value('mileage', mileage.text)
                else:
                    loader.add_value('mileage', "None")
                if transmission != None:
                    loader.add_value('transmission', transmission.text)
                else:
                    loader.add_value('transmission', "None")
                if fuel != None:
                    loader.add_value('fuel', fuel.text)
                else:
                    loader.add_value('fuel', "None")
                if year != None:
                    loader.add_value('year', year.text)
                else:
                    loader.add_value('year', "None")
                if listing != None:
                    loader.add_value('listing_date', listing.text)
                else:
                    loader.add_value('listing_date', "None")
                if location != None:
                    loader.add_value('location', location.text)
                else:
                    loader.add_value('location', "None")
                if postal != None:
                    loader.add_value('postal_code', postal.text)
                else:
                    loader.add_value('postal_code', None)

                yield loader.load_item()
        
process = CrawlerProcess(
    settings = {
        'FEED_URI':'park.jl',
        'FEED_FORMAT':'jsonlines'
    }
)
process.crawl(CountrySpider)
process.start()
