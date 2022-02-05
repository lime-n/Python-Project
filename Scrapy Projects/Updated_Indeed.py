import scrapy
from pandas._libs.internals import defaultdict
from scrapy import Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join
import pandas as pd
from scrapy.crawler import CrawlerProcess
from twocaptcha import TwoCaptcha
import logging
from scrapy.exceptions import CloseSpider 

solver = TwoCaptcha("a93c43d9dd54dc0418fc796ca38e6be2")
sitekey = 'eb27f525-f936-43b4-91e2-95a426d4a8bd'

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
        if response.xpath('//div[@class="slider_container"]') == []:
            try:
                respose =
                result = solver.recaptcha(sitekey=sitekey, url=response.url)
            except Exception as e:
                raise CloseSpider('Could not solve captcha')
                captcha = result.get('code')
                data = {
                    'v': 'f6912ef',
                    'sitekey': 'eb27f525-f936-43b4-91e2-95a426d4a8bd',
                    'host': 'uk.indeed.com',
                    'hl': 'en',
                    'motionData': '{"st":1643925780944,"mm":[[36,77,1643925835957],[38,76,1643925835980],[38,76,1643925836002],[38,76,1643925836092],[38,76,1643925836238],[43,74,1643925836262],[54,72,1643925836283],[75,72,1643925836305],[96,77,1643925836329],[97,77,1643925836565],[70,68,1643925836586],[51,61,1643925836611],[39,57,1643925836632],[33,55,1643925836654],[31,54,1643925836676],[30,53,1643925836699],[30,52,1643925836724],[28,51,1643925836744],[24,48,1643925836766],[21,46,1643925836789],[20,45,1643925836812],[20,44,1643925836834],[20,44,1643925836856],[21,44,1643925836880],[21,44,1643925836948],[21,44,1643925837034],[21,44,1643925837050]],"mm-mp":23.7608695652174,"md":[[21,44,1643925836968]],"md-mp":0,"mu":[[21,44,1643925837067]],"mu-mp":0,"v":1,"topLevel":{"inv":false,"st":1643925780166,"sc":{"availWidth":1600,"availHeight":820,"width":1600,"height":900,"colorDepth":30,"pixelDepth":30,"availLeft":0,"availTop":25},"nv":{"vendorSub":"","productSub":"20030107","vendor":"Google Inc.","maxTouchPoints":0,"userActivation":{},"doNotTrack":null,"geolocation":{},"connection":{},"webkitTemporaryStorage":{},"webkitPersistentStorage":{},"hardwareConcurrency":16,"cookieEnabled":true,"appCodeName":"Mozilla","appName":"Netscape","appVersion":"5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36","platform":"MacIntel","product":"Gecko","userAgent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36","language":"en-GB","languages":["en-GB","en-US","en"],"onLine":true,"webdriver":false,"pdfViewerEnabled":true,"scheduling":{},"bluetooth":{},"clipboard":{},"credentials":{},"keyboard":{},"managed":{},"mediaDevices":{},"storage":{},"serviceWorker":{},"wakeLock":{},"deviceMemory":8,"ink":{},"hid":{},"locks":{},"mediaCapabilities":{},"mediaSession":{},"permissions":{},"presentation":{},"serial":{},"virtualKeyboard":{},"usb":{},"xr":{},"userAgentData":{"brands":[{"brand":" Not;A Brand","version":"99"},{"brand":"Google Chrome","version":"97"},{"brand":"Chromium","version":"97"}],"mobile":false},"plugins":["internal-pdf-viewer","internal-pdf-viewer","internal-pdf-viewer","internal-pdf-viewer","internal-pdf-viewer"]},"dr":"","exec":false,"wn":[],"wn-mp":0,"xy":[],"xy-mp":0,"mm":[[125,141,1643925835259],[74,145,1643925835281],[55,145,1643925835304],[50,144,1643925835327],[55,143,1643925835349],[57,141,1643925835541],[58,139,1643925835563],[57,139,1643925835585],[57,138,1643925835608],[56,138,1643925835630],[53,138,1643925835653],[47,138,1643925835675],[42,137,1643925835698],[38,137,1643925835720],[37,137,1643925835743],[37,136,1643925835765],[37,134,1643925835788],[37,130,1643925835810],[37,123,1643925835833],[37,115,1643925835855],[38,105,1643925835878],[40,96,1643925835900],[42,91,1643925835923],[43,87,1643925835945],[122,88,1643925836339],[138,95,1643925836542],[105,85,1643925836565]],"mm-mp":22.16167664670659,"md":[],"md-mp":416,"mu":[],"mu-mp":0},"session":[],"widgetList":["0ajghniybi76"],"widgetId":"0ajghniybi76","href":"https://uk.indeed.com/jobs?q=graduate+economics&l=Thurrock&fromage=7&filter=0&limit=100","prev":{"escaped":false,"passed":false,"expiredChallenge":false,"expiredResponse":false}}',
                    'n': 'd2ca702c2a957ced82ae9e854121380f717eb61952819558486983cbda5c60ed338bcaff077ed99c5b17a5d1749072f269a4d40ce554044651976343d277a2878576f0fe80bf5f533d9b8ce98008d17759f84a7c818381bd4019c04791f9fd03bfc7df0a851206841049d2c1644daeee6aea8f1c0ace2ce837a4fcb1356b2bf200528d763db6c6186ee7f230a11dbc46333465c957f40915eb9b75f611770e8d0570e0d27ad592700a96ad06e0d36afe3f1555aa0271664eec0efb395efbd316053cc8352f2d3e8336a1290cf52cbb6dff51fa141d318ee5b50e15c4ce050520799007e7a5bb988e3212af356a4ad8059928c5c5f128af0d719ef58ae29d37795e42523f44a933cdadb8ccf02e685c45e11ba98ab939f14c26f44f1b217ceeb278c7f80a2cd27989b4d1cfabc829a2fb297d2d00eedf5f1a33ede18261494d5dac1e7508163ff410e614681c797c923c9c749e0b6f33b0dca7367bdfedd46c53d9cfee65ce96fe76934cf64620f9c37ce427e942335dfeb391bb3cab5e08ecbe398ad2dbb406812f331d2569c8a78b321ff0d02ca7a828b7ba57b7d2fa6720d33b013db3d32af8de3e3c9695b37e083a8d72e79c3bf9dc085a1f0a488bf6578249152823e625cf1ebe2dbb08abca3408c5963696f6bcec1855175cc99649fc9849f8c0002d1bd3a8bde671d2bc806581d10736b3ebcbc06f5e0b61fce70b7ca50a8435cfed682118dca1ee6fde031b8d00b0ec1fe2b5af3b7742983d1ed411c1d1e1db7bed49b23e4365a59bbefb231f30d9e94ae9a60b5c8bb3e42bbf48d0749cb9e90a2705269af502d0de07c9ab8452a40f0bc278aba711ecbc0fe592375f85fbc88a3b451a048182d488b2ef951d3378d9fd02aa88ffb3c6698de2c30934f4a514c04e6e2ba1d48c5e977b3a02eeff83b84ae247748a73580a57c88bdad19c8952b4978fc19c5dc3a074177ded4b3bb531d91f9d3c293a70d6c4c5071b9b533fa9310d2b843cd69851012308b6cbd37f591701dd71e2481c7c3ad84920262332d9acb200e149c8eadccccece2222e2b5329c12dd902a5a7c13d4243b6ec426b9478437616191033336dec57f08695635c94592002a9bbb47982c9c580ee31da8772f261ffb901b3480f32f46ee6620368f8a62e0b0ede528af672de4844fdcaf13f446203e8da8a25d25ccf26deefe19fee51c4de41920190aebb9ccabcd9e21a5a7f9e01a9f70e03e929b10bab69eca3284ef64a774e38d049558739797c10d2bd9422e36eb06acd0fe95c014e67e0414b2a32b82b095cba4fb36754d6d82d82a9e0d16500ae6d34e4ce8d657274e807339fa8368b85707d22236166abf1543baffe2aa65fdac861589b04488e9e2a6780ac6cf66a8e8d0a895446c39e749634fa0beee411157f1c9c0020f1ce81f2ff002d804e0c2b70b98677028c3eb31595e2456e011fe4bd31f45b8b6824d0d44b8ca8df4cd97f178686044a26dd75b9511be20bcb25fd2a484e8a6a05f6d4837ab678e1bae0c1b6edde0b700300d582eaab6aa964cb10d619d4ea85d71e0ed5e0f913948a31e10efe13f6ea75b9f4d24fc7e5c300b3c7ef331845512518855f768c9e64ccb0aae4cacc8cd9ab26b9517341cd6eb62f95a9affe78eab92e29ea5ef7ef2a229782c9e46ffa1ab1d3c4fc1ca100e5d2a74d33faad4ab3dede659fd578e9ebbd514696f22ab190dda8939d20dceae097dd5e861716ce7a80479097aa6df4a36549074211076f44a0dc6e3e08bd2904eff578cc2da7aa3f797c1949c5f16d019dc7a26350ca0e77739767e42c7192ea7a24c1155c358a6712b1050cb960b24fa766dfed00526594eeab2f6254022b3c406c930fe0cfde001b2a66763a3b4afb844de0fda4d97f5163d627c8b2a86616155828b207b604d5820d0ffc6afe5bf5ebe558a3796e90cbcc60516940aad85c7d0d787684ec4e284d7d7c10517cfbad528f20d105723af3e679a9355bdfe35c0c97131a751303c51ba265234510d5398e1162f357c87308dcfc9966e26a6a0492d977bc3bc502d1bc4c0260058930f1284eacfd10f51ef3ec901581922f912e4e4f4c3d90a08e7c63703a91679d1afde2061b18447139719d0b4595cb7105cfca96f5bef3df8974c08f4f45b8a0968ef3635c72c6d7404e6c01bc2b5dce177ecc51af3bb037e445b488801d44e8703e23857993a1893342ba4443a2613e233aa2b9b31070d551a1f2dfa4b65f90372ffbe7e8a3119d0573e44f2cf491dcb918a9faeb7a9d539e370399249ca13e54d728cec70e3d396fc1c48ecea694af2ebd900',
                    'c': '{"type":"hsw","req":"%s"}'% captcha
}
        else:
            pass
        

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