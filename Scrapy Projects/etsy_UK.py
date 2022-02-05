import scrapy
from scrapy.item import Field
from scrapy.crawler import CrawlerProcess
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst
import pandas as pd
from bs4 import BeautifulSoup
import re

headers = {
    'authority': 'www.etsy.com',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
    'x-csrf-token': '3:1641889873:_Q-39PtfFKMySNfi_h--QbxYHCm1:855267979a3c1daef03cb79019135c6263f1909e30ca4daa74ade094495563c4',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'accept': '*/*',
    'x-requested-with': 'XMLHttpRequest',
    'x-page-guid': 'eeed2a06ed9.1ef555f177c8d1fd2b0f.00',
    'x-detected-locale': 'GBP|en-GB|GB',
    'sec-ch-ua-platform': '"macOS"',
    'origin': 'https://www.etsy.com',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://www.etsy.com/search?explicit=1&q=30s&locationQuery=2635167&ship_to=GB&page=2&ref=pagination',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
}
cookies = {
    'uaid': 'tY0Oqvo_qVkYxdriXXUBvPrQdARjZACCxCtmT2F0tVJpYmaKkpWSd1WOY1haWYZTlk9gbpBbVoRXRVapUXFFTkS8p1ItAwA.',
    'user_prefs': '93RQBOPpfc683vzro_QXvEclWOxjZACCxCtmT2F0tJK7U4CSTl5pTo6OUmqerruTko4SiACLGEEoXEQsAwA.',
    'fve': '1641297637.0',
    'ua': '531227642bc86f3b5fd7103a0c0b4fd6',
    'p': 'eyJnZHByX3RwIjoxLCJnZHByX3AiOjF9',
    '_gcl_au': '1.1.353405131.1641297656',
    '__adal_ca': 'so%3Ddirect%26me%3Dnone%26ca%3Ddirect%26co%3D%28not%2520set%29%26ke%3D%28not%2520set%29',
    '__adal_cw': '1641297656536',
    '_pin_unauth': 'dWlkPU5EVXhaV0V3WW1VdE9XSm1aQzAwWVRVM0xXSmxZbUV0TkRobFl6Rm1NREJqTmpobQ',
    'exp_hangover': 'oZ5BpavYhL-JYQA4a1X3wr_N0f5jZACCxCtmT8H0tSOzqpXKU5PiE4tKMtMykzMTc-JzEktS85Ir4wtN4o0MDC2VrJQy81JzMtMzk3JSlWoZAA..',
    '_gid': 'GA1.2.1681521733.1641810113',
    'pla_spr': '0',
    '_ga': 'GA1.1.1030534579.1641297656',
    '_uetsid': '217eb49071ff11ecbf218dc9c983ae44',
    '_uetvid': 'f961cf606d5511ecbf0e97652de51722',
    '__adal_ses': '*',
    '__adal_id': 'dd77ab17-e209-4c7f-89c8-24391b08fc76.1641297657.12.1641889874.1641832214.d777914c-55c7-49ef-994e-cbf20ccbecd5',
    '_tq_id.TV-27270909-1.a4d5': '00777aa07367fe91.1641297657.0.1641889875..',
    'granify.uuid': 'b59b53ac-a77e-4fef-8ea7-73d9b383b70d',
    '_ga_KR3J610VYM': 'GS1.1.1641889873.17.0.1641889938.60',
    'granify.session.QrsCf': '-1',
}

class Etsy2Item(scrapy.Item):
    price = Field(output_processor = TakeFirst())
    title = Field(output_processor = TakeFirst())
    review = Field(output_processor = TakeFirst())
    category = Field(output_processor = TakeFirst())
    gender = Field(output_processor = TakeFirst())
    decade = Field(output_processor = TakeFirst())
    stars = Field(output_processor = TakeFirst())


class Etsy2Spider(scrapy.Spider):
    name = 'Etsy2'
    data = pd.read_csv('/Volumes/Seagate/online downloads/etsy.csv')
    start_urls = ['https://www.etsy.com/api/v3/ajax/bespoke/member/neu/specs/async_search_results']

    custom_settings = {
        'User_Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
        #'DOWNLOAD_DELAY':3
    }

    def start_requests(self):
        for key, value, page, gender, category in zip(self.data['keys'].values, self.data['values'].values,self.data['pages'].values,self.data['gender'].values,self.data['category'].values):
            for urls in self.start_urls:
                for p in range(1, page+1):
                    yield scrapy.FormRequest(
                        urls,
                        method='POST',
                        headers = headers,
                        cookies=cookies,
                        formdata = {
                          'log_performance_metrics': 'true',
                          'specs[async_search_results][]': 'Search2_ApiSpecs_WebSearch',
                          'specs[async_search_results][1][search_request_params][detected_locale][language]': 'en-GB',
                          'specs[async_search_results][1][search_request_params][detected_locale][currency_code]': 'GBP',
                          'specs[async_search_results][1][search_request_params][detected_locale][region]': 'GB',
                          'specs[async_search_results][1][search_request_params][locale][language]': 'en-GB',
                          'specs[async_search_results][1][search_request_params][locale][currency_code]': 'GBP',
                          'specs[async_search_results][1][search_request_params][locale][region]': 'GB',
                          'specs[async_search_results][1][search_request_params][name_map][query]': 'q',
                          'specs[async_search_results][1][search_request_params][name_map][query_type]': 'qt',
                          'specs[async_search_results][1][search_request_params][name_map][results_per_page]': 'result_count',
                          'specs[async_search_results][1][search_request_params][name_map][min_price]': 'min',
                          'specs[async_search_results][1][search_request_params][name_map][max_price]': 'max',
                          'specs[async_search_results][1][search_request_params][parameters][q]': value,
                          'specs[async_search_results][1][search_request_params][parameters][explicit]': '1',
                          'specs[async_search_results][1][search_request_params][parameters][locationQuery]': '2635167',
                          'specs[async_search_results][1][search_request_params][parameters][ship_to]': 'GB',
                          'specs[async_search_results][1][search_request_params][parameters][page]': str(p+1) ,
                          'specs[async_search_results][1][search_request_params][parameters][ref]': 'pagination',
                          'specs[async_search_results][1][search_request_params][parameters][facet]': key,
                          'specs[async_search_results][1][search_request_params][parameters][referrer]': f'https://www.etsy.com/search/{key}/?q={value}&explicit=1&locationQuery=2635167&ship_to=GB&page={p}&ref=pagination',
                          'specs[async_search_results][1][search_request_params][user_id]': '',
                          'specs[async_search_results][1][request_type]': 'pagination_preact',
                          'specs[async_search_results][1][is_eligible_for_spa_reformulations]': 'false',
                          'view_data_event_name': 'search_async_pagination_specview_rendered'
                        },
                        callback = self.parse,
                        cb_kwargs = {
                            'gender':gender,
                            'category':category,
                            'decade':value,
                        }
                            )
    def parse(self, response, gender, category, decade):
        output = response.json().get('output')
        #test = {'Price':[], 'Title':[], 'Review':[]}
        for values in output.values():
            soup = BeautifulSoup(values)
            price=soup.select('li:nth-child(n) > div:nth-child(1) > div:nth-child(1) > a:nth-child(1) > div:nth-child(2) > div:nth-child(1) > div:nth-child(4) > p:nth-child(1) > span:nth-child(2)')
            title = soup.select('li:nth-child(n) div:nth-child(1) div:nth-child(1) a:nth-child(1) div:nth-child(2) div:nth-child(1) h3:nth-child(1)')
            review = soup.select('li:nth-child(n) > div:nth-child(1) > div:nth-child(1) > a:nth-child(1) > div:nth-child(2) > div:nth-child(1) > div:nth-child(3) > span:nth-child(1) > span:nth-child(2)')
            #discount = soup.select('li:nth-child(n) > div:nth-child(1) > div:nth-child(1) > a:nth-child(1) > div:nth-child(2) > div:nth-child(1) > div:nth-child(4) > p:nth-child(2) > span:nth-child(1) > span:nth-child(2)')
            stars = soup.select('li:nth-child(n) > div:nth-child(1) > div:nth-child(1) > a:nth-child(1) > div:nth-child(2) > div:nth-child(1) > div:nth-child(3) > span:nth-child(1) > span:nth-child(1) > input:nth-child(2)[value]')
            for P, T, RE, S in zip(price, title, review, stars):
                
                pp = P.text.strip()
                ra = T.text.strip()
                red = int(re.sub(',','',RE.text.strip().strip('()')))
                sa = S['value']


                loader = ItemLoader(Etsy2Item())

                loader.add_value('review', red)
                loader.add_value('price', pp)
                loader.add_value('title', ra)
                loader.add_value('stars', sa)
                loader.add_value('gender', gender)
                loader.add_value('category', category)
                loader.add_value('decade', decade)

                
                yield loader.load_item()


            
process = CrawlerProcess(
    settings = {
        'FEED_URI':'etsy_js_UK.jl',
        'FEED_FORMAT':'jsonlines'
    }
)
process.crawl(Etsy2Spider)
process.start()