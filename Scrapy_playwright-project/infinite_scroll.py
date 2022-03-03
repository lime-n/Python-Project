from playwright.async_api import Response as PlaywrightResponse
from scrapy_playwright.page import PageCoroutine
from scrapy import Spider, Request
import jsonlines
import pandas as pd

cookies = {"Your cookies here"}

headers = {"Your headers here"}

class EventSpider(Spider):
    name = "event_c"

    def start_requests(self):
        yield Request(
            url="https://www.tiktok.com/tag/tesla?lang=en",
            headers=headers,
            cookies=cookies,
            meta=dict(
                playwright=True,
                playwright_page_coroutines = [
                    #PageCoroutine("wait_for_selector", ".sc-fmjce0-0.ergZdM"),
                    PageCoroutine("evaluate", """setInterval(function () {
                                    var scrollingElement = (document.scrollingElement || document.body);
                                    scrollingElement.scrollTop = scrollingElement.scrollHeight;
                                }, 200);"""),
                    PageCoroutine("wait_for_timeout", 180000)
                ],
                playwright_page_event_handlers={
                    "response": "handle_response",
                },
            ),
        )

    async def handle_response(self, response: PlaywrightResponse) -> None:
        """
        We can grab the post data with response.request.post - there are three different types for different needs.
        The method below helps grab those resource types of 'xhr' and 'fetch' until I can work out how to only send these to the download request.
        """
        self.logger.info(f'test the log of data: {response.request.resource_type, response.request.url, response.request.method}')
        jl_file = "test.jl"
        #data = defaultdict(list)
        data = {}
        if response.request.resource_type == "xhr":
            if response.request.method == "POST":
                if 'api' in response.request.url:
                    data['resource_type']=response.request.resource_type,
                    data['request_url']=response.request.url,
                    data['post_data']=response.request.post_data_json,
                    data['method']=response.request.method
                    with jsonlines.open(jl_file, mode='a') as writer:
                        writer.write(data)
        else: 
            if response.request.method == "GET":
                if response.request.resource_type == "fetch":
                    if "api" in response.request.url:
                        url_with_payload = response.request.url.split("/?")[-1]
                        url_without_payload = response.request.url.split("/?")[0]
                        data['resource_type']=response.request.resource_type,
                        data['request_url']=url_without_payload,
                        data['post_data']=url_with_payload,
                        data['method']=response.request.method
                        with jsonlines.open(jl_file, mode='a') as writer:
                            writer.write(data)

    def parse(self, response):
        data = pd.read_json('test.jl', lines=True)
        """
        You can then send requests here using those results above.
        """
        yield {
            'data':data
        }
