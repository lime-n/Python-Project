import scrapy
from scrapy.item import Field
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose, Join
from scrapy.crawler import CrawlerProcess
from price_parser import Price
from collections import defaultdict

def get_price(price_raw):
    price_object = Price.fromstring(price_raw)
    return price_object.amount_float

def get_currency(price_raw):
    price_object = Price.fromstring(price_raw)
    currency = price_object.currency
    return currency

headers = {
    'authority': 'www.discogs.com',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'accept': 'text/html, */*; q=0.01',
    'x-requested-with': 'XMLHttpRequest',
    'x-pjax': 'true',
    'x-pjax-container': '#pjax_container',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://www.discogs.com/sell/list?format=Vinyl',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cookie': 'sid=e82fd8c1941b6d8337152bb900b24337; mp_session=99a73329a576174b01b53879; _ga=GA1.2.1297331274.1643639856; OptanonAlertBoxClosed=2022-01-31T14:37:37.934Z; eupubconsent-v2=CPTr4niPTr4niAcABBENCACgAAAAAAAAACiQAAAAAAAA.YAAAAAAAAAAA; language2=en; __cf_bm=07QAxH1XhBKdPBg1_8fUnk1pQUuymsy61_B5A6ftZOw-1643916724-0-AbNqtyDRAP4X73pq1CkmMi1b6xrWJqWT6H18uW7YKy6hm4/53vgpoOxQ4g5rdeIAQ34ecrfk4H4Gac+X4RfFQRs=; _gid=GA1.2.748794408.1643916727; _pbjs_userid_consent_data=3524755945110770; lngtd-sdp=8; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Feb+03+2022+19%3A33%3A03+GMT%2B0000+(Greenwich+Mean+Time)&version=6.20.0&isIABGlobal=false&hosts=&consentId=5d125622-f6ca-4306-a268-25b4c52a41f2&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0004%3A0%2CC0003%3A0%2CC0002%3A0%2CSTACK8%3A0&geolocation=GB%3BENG&AwaitingReconsent=false',
}

class VinylItem(scrapy.Item):
    title = Field(output_processor = TakeFirst())
    label = Field()
    media_condition=Field(input_processor = MapCompose(str.strip),
    output_processor = TakeFirst())
    sleeve_condition = Field(output_processor = TakeFirst())
    location = Field(input_processor = MapCompose(str.strip),
                        output_processor = Join())
    price = Field(input_processor = MapCompose(get_price)
        ,output_processor = TakeFirst())
    currency = Field(input_processor = MapCompose(get_currency)
        ,output_processor  = TakeFirst())
    rated = Field(input_processor = MapCompose(str.strip)
        ,output_processor = Join())
    have_vinyl = Field(output_processor = TakeFirst())
    want_vinyl = Field(output_processor = TakeFirst())
    format = Field(input_processor = MapCompose(str.strip),
    output_processor = Join())
    released = Field(input_processor = MapCompose(str.strip),
    output_processor = Join())
    genre = Field(input_processor = MapCompose(str.strip),
    output_processor = Join())
    style = Field(input_processor = MapCompose(str.strip),
    output_processor = Join())

class VinylSpider(scrapy.Spider):
    name = 'vinyl'
    #allowed_domains = ['x']
    currency = ['EUR', 'GBP', 'US']
    start_urls = defaultdict(list)
    for i in range(1950, 2021, 1):
        start_urls["EUR"].append(f"https://www.discogs.com/sell/list?format=Vinyl&currency=EUR&year={i}")
        start_urls["GBP"].append(f"https://www.discogs.com/sell/list?format=Vinyl&currency=GBP&year={i}")
        start_urls["USD"].append(f"https://www.discogs.com/sell/list?format=Vinyl&currency=USD&year={i}")

    custom_settings = {
        #'LOG_LEVEL':'INFO',
        'REACTOR_THREADPOOL_MAXSIZE':25,
        'CONCURRENT_REQUESTS_PER_DOMAIN':24,
        'CONCURRENT_REQUESTS':32,
        'COOKIES_ENABLED':False,
        'DOWNLOAD_TIMEOUT':15,
        'HTTPERROR_ALLOW_ALL':TRUE
    }

    def start_requests(self):
        for values in self.start_urls.values():
            for url in values:
                yield scrapy.Request(
                url, callback = self.parse,headers=headers
            )

    def parse(self, response):
        content = response.xpath("//table[@class='table_block mpitems push_down table_responsive']//tbody//tr")
        for items in content:
            loader = ItemLoader(VinylItem(), selector = items)
            loader.add_xpath('title', "(.//strong//a)[position() mod 2=1]//text()")
            loader.add_xpath('label', './/p[@class="hide_mobile label_and_cat"]//a//text()')
            loader.add_xpath("media_condition", '(.//p[@class="item_condition"]//span)[position() mod 3=0]//text()')
            loader.add_xpath("sleeve_condition", './/p[@class="item_condition"]//span[@class="item_sleeve_condition"]//text()')
            loader.add_xpath("location", '(.//td[@class="seller_info"]//li)[position() mod 3=0]//text()')
            loader.add_xpath('price', '(//tbody//tr//td//span[@class="price"])[position() mod 2=0]//text()')
            loader.add_xpath('currency', '(//tbody//tr//td//span[@class="price"])[position() mod 2=0]//text()')

            if loader.add_xpath('rated', './/td//div[@class="community_rating"]//text()'):
                loader.add_xpath('rated', './/td//div[@class="community_rating"]//text()')
            else:
                loader.add_xpath('rated', "None")

            if loader.add_xpath('have_vinyl', '(.//td//div[@class="community_result"]//span[@class="community_label"])[contains(text(),"have")]//text()'):
                loader.add_xpath('have_vinyl', '(.//td//div[@class="community_result"]//span[@class="community_label"])[contains(text(),"have")]//text()')
            else:
                loader.add_xpath('have_vinyl', 'None')

            if loader.add_xpath('want_vinyl', '(.//td//div[@class="community_result"]//span[@class="community_label"])[contains(text(),"want")]//text()'):
                loader.add_xpath('want_vinyl', '(.//td//div[@class="community_result"]//span[@class="community_label"])[contains(text(),"want")]//text()')
            else:
                loader.add_xpath('want_vinyl', 'None')

            links = items.xpath('.//td[@class="item_description"]//strong//@href').get()
            
            yield response.follow(
                response.urljoin(links), 
                callback = self.parse_vinyls,
                cb_kwargs = {
                    'loader':loader
                }
            )            
        next_page = response.xpath('(//ul[@class="pagination_page_links"]//a)[last()]//@href').get()
        if next_page:
            yield response.follow(
                response.urljoin(next_page),headers=headers,
                callback = self.parse
                
            )

    def parse_vinyls(self, response, loader):
        #loader = ItemLoader(VinylItem(), selector = response)
        loader.add_value('format', response.xpath("(.//div[@id='page_content']//div[5])[1]//text()").get())
        loader.add_value('released', response.xpath("(.//div[@id='page_content']//div[9])[1]//text()").get())
        loader.add_value('genre', response.xpath("(.//div[@id='page_content']//div[11])[1]//text()").get())
        loader.add_value('style', response.xpath("(.//div[@id='page_content']//div[13])[1]//text()").get())
        yield loader.load_item()

process = CrawlerProcess(
    settings = {
        'FEED_URI':'vinyl.jl',
        'FEED_FORMAT':'jsonlines'
    }
)
process.crawl(VinylSpider)
process.start()
