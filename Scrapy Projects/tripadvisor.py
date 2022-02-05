import json
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose
import pandas as pd
from bs4 import BeautifulSoup
import logging

class RestaurantReviews(scrapy.Item):
    
    restaurant_geo = scrapy.Field(output_processor=TakeFirst())
    review_div = scrapy.Field(
        input_processor = MapCompose(lambda x : str(x)),
        output_processor=TakeFirst()
    )
    name = scrapy.Field(output_processor=TakeFirst())

class RestaurantReview(scrapy.Spider):
    name = 'restaurant_reviews'

    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'ROBOTSTXT_OBEY': False
    }

    offset = {}
    MAX_REVIEWS = 1000
    cntr = 0

    df = pd.read_excel('links.xlsx')
    start_urls = df.set_index('Hotels and Restaurant').to_dict('index')

    

    # start_urls = {
    #     # 'restaurant': 'https://www.tripadvisor.co.uk/Restaurant_Review-g186527-d3708389-Reviews-Chianti-Falkirk_Falkirk_District_Scotland.html',
    #     # 'hotel': 'https://www.tripadvisor.co.uk/Hotel_Review-g1097176-d570642-Reviews-Premier_Inn_Falkirk_East_hotel-Polmont_Falkirk_District_Scotland.html'
    #     'hotel': 'https://www.tripadvisor.co.uk/Attraction_Review-g186419-d216438-Reviews-Windsor_Castle-Windsor_Windsor_and_Maidenhead_Berkshire_England.html'
    # }

    def start_requests(self):
        self.logger.setLevel(logging.INFO)
        # self.start_urls = json.loads(self.start_urls)
        for name, items in self.start_urls.items():
            
            restaurant_geo = items['Type']
            url = items['urls']
            self.offset.setdefault(restaurant_geo, 0)
            yield scrapy.Request(
                url,
                callback=self.get_restaurant_reviews,
                cb_kwargs={
                    'name': name,
                    'restaurant_geo': restaurant_geo,
                    'first_page': True,
                    'first_review_page': False
                }
            )

    def get_restaurant_reviews(self, response, restaurant_geo, first_page, first_review_page, name):

        if restaurant_geo == 'H' and first_page == True:
            for item in self.parse_reviews(
                response,
                restaurant_geo,
                first_page,
                first_review_page,
                name
            ):
                yield item
            return

        headers = {
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://www.tripadvisor.co.uk',
            'x-requested-with': 'XMLHttpRequest'
        }
        body = 'preferFriendReviews=FALSE&t=&q=&filterSeasons=&filterLang=ALL&filterSafety=FALSE'\
            '&filterSegment=&trating=&reqNum=1&isLastPoll=false&paramSeqId=1&waitTime=50&changeSet=REVIEW_LIST'

        url = response.url if isinstance(response, scrapy.http.response.html.HtmlResponse) else response

        yield scrapy.Request(
            url,
            method='POST',
            body=body,
            headers=headers,
            callback=self.parse_reviews,
            cb_kwargs={
                'restaurant_geo': restaurant_geo,
                'first_page': first_page,
                'first_review_page': first_review_page,
                'name': name
            }
        )

    def parse_reviews(self, response, restaurant_geo, first_page, first_review_page, name):

        if first_page:
            for item in self.get_page(response, restaurant_geo, name):
                yield item
            return

        if first_review_page:
            for item in self.page_number_check(response, restaurant_geo, name):
                yield item

        if self.offset[restaurant_geo] >= self.MAX_REVIEWS:
            return

        # parse reviews
        reviews = response.xpath("//div[@class='review-container']").getall()
        for review in reviews:
            loader = ItemLoader(RestaurantReviews())
            loader.add_value('restaurant_geo', restaurant_geo)
            loader.add_value('review_div', str(review))
            loader.add_value('name', name)

            self.offset[restaurant_geo] += 1

            yield loader.load_item()

        # get next page
        next_page = response.xpath("//a[@class='nav next ui_button primary' or @class='nav next ']/@href").get()
        if next_page:
            # href = response.xpath("(//div[@class='review-container'])[last()]//@href").get()
            next_url = response.urljoin(next_page)
            yield scrapy.Request(
                next_url,
                callback=self.get_restaurant_reviews,
                cb_kwargs={
                    'restaurant_geo': restaurant_geo,
                    'first_page': False,
                    'first_review_page': False,
                    'name': name
                }
            )

    def page_number_check(self, response, restaurant_geo, name):
        """
        Check if the current review page number is the same as the first
        review page number.
        """

        page_one_url = response.xpath("//div[@class='unified ui_pagination ' or @class='pageNumbers']//a[@data-offset=0]/@href").get()
        if page_one_url:
            if page_one_url != response.url:
                url = response.urljoin(page_one_url)
                for item in self.get_restaurant_reviews(
                    url,
                    restaurant_geo,
                    first_page=False,
                    first_review_page=False,
                    name=name
                ):
                    yield item
                    return

        yield response


    def get_page(self, response, restaurant_geo, name):

        # get first review link
        xpath = "//div[@class='prw_rup prw_reviews_review_resp']//div[@class='quote']/a/@href"
        if restaurant_geo == 'H':
            xpath = "//div[@data-test-target='review-title']//a/@href"

        link = response.xpath(xpath).get()
        if link:
            url = response.urljoin(link)
            for item in self.get_restaurant_reviews(
                url,
                restaurant_geo,
                first_page=False,
                first_review_page=True,
                name=name
            ):
                yield item


process = CrawlerProcess(
    settings={
        'FEED_URI': 'output.json',
        'FEED_FORMAT': 'jsonlines'
    }
)
process.crawl(RestaurantReview)
process.start()
