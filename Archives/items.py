# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from itemloaders.processors import MapCompose

class InstaVmItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
class DownfilesItem(scrapy.Item):
    
    # define the fields for your item here like:
    file_urls = scrapy.Field(input_processor = MapCompose(lambda x: x+'.png'))
    original_file_name = scrapy.Field()
    files = scrapy.Field
