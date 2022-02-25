import scrapy
from scrapy.item import Field
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose, Join
import re
import pandas as pd
from price_parser import Price

def get_price(price_raw):
    price_object = Price.fromstring(price_raw)
    return round(price_object.amount_float)

def get_currency(price_raw):
    price_object = Price.fromstring(price_raw)
    currency = price_object.currency
    return currency

list_of_words = [
    'Oil', 
    'acrylic',
    'enamel',
    'canvas',
    'print',
    'paper',
    'Watercolour',
    'Velin',
    'paint',
    'Sculpture',
    'acylic',
    'wire',
    'porcelain',
    'Pencil',
    'Inkjet',
    'Hahnemuhle',
    'board',
    'wood',
    'screenprint',
    'plaster',
    'concrete',
    'laser',
    'charcoal',
    'cement',
    'casting',
    'copper'

]

diameter_word = ['diam']

"""
Methodology: To explain how I got the values above - 
We send art_features to the item-loader by using a unique symbol in MapCompose to adjoin the strings. 
Afterwards, by filtering on an external platform to grab the unique names belonging to the media only. We return to scrapy,
then use these keywords to grab only those strings which match. 

"""

class examItem(scrapy.Item):
    url = Field(output_processor = TakeFirst())
    title = Field(input_processor = MapCompose(str.strip), output_processor = Join())
    media = Field(input_processor = MapCompose(str.strip), output_processor = Join())
    height_cm = Field(input_processor = MapCompose(float),output_processor = TakeFirst())
    diameter = Field(input_processor = MapCompose(int),output_processor = TakeFirst())
    depth_cm = Field(input_processor = MapCompose(int),output_processor = TakeFirst())
    width_cm = Field(input_processor = MapCompose(float),output_processor = TakeFirst())
    price_gbp = Field(input_processor = MapCompose(get_price),output_processor = TakeFirst())
    currency = Field(input_processor = MapCompose(get_currency),output_processor = TakeFirst())


class examSpider(scrapy.Spider):
    name = 'art'
    start_urls = []
    for i in range(1, 10):
       start_urls.append(f'https://www.bearspace.co.uk/purchase?page={i+1}')

    def parse(self, response):
        container = response.xpath('//section[@aria-label="Product Gallery"]//li')
        for art_links in container:
            links = art_links.xpath('.//a//@href').get()
            art_name = links.split("/")[-1]
            art_name = " ".join(art_name.split("-"))
            yield response.follow(
                url=links,
                callback = self.parse_artwork,
                cb_kwargs = {
                    'url_artwork':links,
                    'artName':art_name
                }
            )
    def parse_artwork(self, response, url_artwork, artName):
        container = response.xpath('//article[@class="_30ZY-"]')
        for art_details in container:

            """ Parse for the following values:
                * title, media, height, width, price
                Then upload onto itemloaders for efficiency
            """
            price = response.xpath("//span[@data-hook='formatted-primary-price']//text()").get()
            price = price.split(".")[0]

            
            loader = ItemLoader(examItem(), selector = art_details)
            loader.add_value('url', url_artwork)
            loader.add_value('title', artName)
            loader.add_value('price_gbp',price)
            loader.add_value('currency',price)

            art_features = response.xpath("(//pre[@class='_28cEs']/p//text())[position() <4]").getall()
            
            """"
            The next method:
            After filtering for keywords, we find those keywords in the string by using pandas str.contains function
            """
            test = [x.lower() for x in art_features]
            df = pd.DataFrame(test)
            media = df[df[0].str.contains('|'.join(list_of_words), na=False, case=False)]
            for val in media[0].values:

                loader.add_value('media', val)

            """
            The next method:
            Use a regex pattern to extract only those values with decimals, and belonging to the height/width/depth dstructure.
            We escape the value k, because some strings are assigned likeso: 88k - which is not what we want to collect.

            Furthermore, a few listings only have values for diameter, these were also extracted.

            This method was optimised because it was parsed following the first method above. Using a smaller sample to work with can go a long-way.
            """
            for num in df[0].values:
                len_num = len(re.findall('\d+(?:\.\d+)?k|(\d+(?:\.\d+)?)', num))
                if len_num >= 2:
                    if re.findall('\d+(?:\.\d+)?k|(\d+(?:\.\d+)?)', num)[0]:
                        height = re.findall('\d+(?:\.\d+)?k|(\d+(?:\.\d+)?)', num)[0]
                        loader.add_value('height_cm', height)
                    else:
                        loader.add_value('height_cm', None)
                    if re.findall('\d+(?:\.\d+)?k|(\d+(?:\.\d+)?)', num)[1]:
                        width = re.findall('\d+(?:\.\d+)?k|(\d+(?:\.\d+)?)', num)[1]
                        width = width.split("  ")[0]
                        loader.add_value('width_cm', width)
                    else:
                        loader.add_value('width_cm', None)
                if 'diam' in num:
                    diam_ = re.findall('[0-9]', num)
                    loader.add_value('diameter', ''.join(diam_))
                else:
                    loader.add_value('diameter', None)
                if len_num == 3:
                    depth_cm = re.findall('\d+(?:\.\d+)?k|(\d+(?:\.\d+)?)', num)[2]
                    loader.add_value('depth_cm', depth_cm)
                

            yield loader.load_item()
