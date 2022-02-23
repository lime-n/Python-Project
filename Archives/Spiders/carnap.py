import scrapy
from bs4 import BeautifulSoup
import json
import re
from insta_vm.items import DownfilesItem

headers = {
    'Connection': 'keep-alive',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'Accept': '*/*',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'sec-ch-ua-platform': '"macOS"',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://digital.library.pitt.edu/islandora/object/pitt%3A31735061815696/viewer',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
}

class carnapSpider(scrapy.Spider):
    name = 'carnap'
    start_urls = []
    for pages in range(1, 44):
        start_urls.append(f'https://digital.library.pitt.edu/collection/archives-scientific-philosophy?page={pages}&islandora_solr_search_navigation=0&f%5B0%5D=mods_relatedItem_host_titleInfo_title_ms%3A%22Rudolf%5C%20Carnap%5C%20Papers%22')

    custom_settings = {
        'USER_AGENT':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
    }
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url, headers=headers,
                callback = self.parse
            )
    
    def parse(self, response):
        container = response.xpath("//div[@class='islandora islandora-solr-search-results']/div")
        for data in container:
            href_data = data.xpath('(.//a)[position() mod 5=1]//@href').get()
            href_data = '/viewer#'.join(href_data.split("#"))
            links = response.urljoin(href_data)
            loader = ItemLoader(carnapItem())
            loader.add_value('links', links)
            yield loader.load_item()

    def parse(self, response):
        container = response.xpath("//div[@class='islandora islandora-solr-search-results']/div")
        for data in container:
            href_data = data.xpath('(.//a)[position() mod 5=1]//@href').get()
            href_data = '/viewer#'.join(href_data.split("#"))
            links = response.urljoin(href_data)
            yield response.follow(url=links, callback = self.parse_carnap, headers=headers)
        
    def parse_carnap(self, response):
        """ Parse the website with Beautiful Soup and Json,  
            by grabbing the ID for each link and it's title name
        """
        soup = BeautifulSoup(response.body, 'lxml')
        for i in range(53, 54, 1):
            java_val= soup.select(f"*[type]:nth-child({i})")
            for b in java_val:
                data_test=b.text[b.text.find('{'):b.text.rfind('}')+1]
                data_test = json.loads(data_test)
                test = BeautifulSoup(data_test['islandoraInternetArchiveBookReader']['info'], 'lxml')
                title = re.sub('Title','',test.find('tr', {'class':'odd'}).text)
                id_no = [str(test.select('.even')[1]).split('>')[4].split("<")[0]]
                page_count = data_test['islandoraInternetArchiveBookReader']['pageCount']
                for id_m in id_no:
                    for pg in range(1, page_count+1):
                        """ grab the correct naming conventions to later convert the file names 
                            ---- next stage is to convert these images into a pdf.
                        """
                        another_str=f'https://digital.library.pitt.edu/internet_archive_bookreader_get_image_uri/pitt:{id_m}-00{str(pg).zfill(2)}'
                        id_url f'{id_m}-00{str(pg).zfill(2)}'
                        yield scrapy.Request(
                            url = another_str,
                            method='POST',
                            headers=headers,
                            callback = self.parse_images,
                            cb_kwargs = {
                                'title':title,
                                'id_url':id_url}
                             )
        
    def parse_images(self, response, title, id_url):
        file_url = response.text
        item = DownfilesItem()
        item['file_urls'] = file_url
        item['id_url'] = id_url
        item['original_file_name'] = title
        yield item


