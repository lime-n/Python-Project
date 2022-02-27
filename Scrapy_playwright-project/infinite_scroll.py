from playwright.async_api import Response as PlaywrightResponse
from scrapy_playwright.page import PageCoroutine
from scrapy import Spider, Request
import jsonlines
import pandas as pd

cookies = {
    'tt_csrf_token': 'EiL919jG7Lu8pBchUrs5UteQ',
    '_abck': '8EC24756EB1912EC89660F84168635B9~-1~YAAQF8kYPuy/USB/AQAAX3r4Owe6obCpSuRjKCgai1DoqNtQjLxtF4VtSbUibFN2W0txH4w3UtnfdlS+GTrNC5lPsxsnGbR9mlWEJlZ23fKkSSKKJakYvc/Gp/iRPSC7SleSwMc6KyqFVoSO84jNkkDHhBXVJW+nfa6zr+xa3mV0jbvg6NosmvR2ulIRl7mdcJ3k5aKyvPXmBrS+PsIxM6zQVrZ7L8BMc3XvpmOT7GRsxfU+Cv29nMwF2E6Tys2k+8dfcAACyMshItyluo5XL3dxgfbwSN1fpnIkyT8fdEVc4jhLogJS/IdeAm2PrpIo191jRLTjl0xjTJuF3UHtoplVMMgo9uePnxGOC0mR+4w580Jc884Z3DrjKaw=~-1~-1~-1',
    'ak_bmsc': '0483933D7814F80C5FC9340CF3149423~000000000000000000000000000000~YAAQF8kYPu2/USB/AQAAX3r4Ow7meFI9Gktxy11FyUw9oMqCwmtnqlC5XXqm/x7CGlO6M1YAV78dKnCkb7KPv4WsRP55K4j19rHnW3z3fUsKezjHZLkPe964zvj9wjS0PSpkY9q5d1OOMEbfxT1kMaBm1WfGrXVd52DGkW7oTp/oj8VBQ21DWZb6aYvCTYMapocCliJpGAV84Oeq72EJcbrIH03RHVzEoHryNRUbTU6AWyVnN6rkXiQvNr7p5+XYVDXlhcnXwDsVJUKSoLHozOXvSFjT6j8ZP50ELLxVNgtO7qX0jqnuFCtx4LA7/N5yx2aEpN1vzNH9toU5YJyA6mVFqyHfTzzGDKI/TCfPPG3QKOe4Am9sAr4nWRPBYRfD/UK9Bhi/5Rg=',
    'bm_sz': '6D63B0ED8EABF849F5CF46267022A9B4~YAAQF8kYPu6/USB/AQAAX3r4Ow70olItIzwTNAIx24ar8guNpiJkyWrgz+W9kyLzSIMUV2ZSyyqH2jD/E35j04K1AKRSGlUfGFRSqG/QscbXr3GUhjMMOrXEpg+NLkaS4fwi0o7VVRGIazXg3JP8nxh9oCUkjpUddvTY1Nf3AyHucZURp+dB9Bo2FiBoxDjQqNdvwuBDnORrdD8UKXcxMj0rhThLoXQit5Maxl71lvqEWcnlMRvy6cc1spu4rnVKi3BbBdCIYJN8SEDeuHDXiaGeXXSU8lwrBIBbPFI5o0KEw/w=~4535603~3753539',
    'ttwid': '1%7CLM6CFlqe_e7GdiPAm--oyYIrMgG1O2t0sU-hyxv_qgk%7C1645978658%7Cf5197c256136086c7d4ff9034d0cc377ae7ab53f75de08e0baa789d764134477',
    'msToken': 'mcXx3w4j4lwOoDlmiDlwSpi5sOdQlWk3wJ7MZN_NNSir5mTmbUDSlaZHJdjmLz88oXB_qB6cntkHhkZf9a4bX4v4EnaHu1zhLZW5llMz-RzKdye2GYxkVVPH2-rGbV4Mzh4CERo=',
    'bm_sv': '10EA58DBCE4B15EDA6DD11F1850A92CB~lAYym7f+NNKqQDiGadyaC7pGF53DFiRMaTviLd+3iyrdK33Uom0sxg1mOBFiRH5VVJ43Wyjii/C0n8yJuq+zjaTtbtgDW2DFrejC4Cpl8YmTDfAahWBg0jFTiY+M0WmRaAXaFuvQ3RYOmT02vafq1iJmdJJWYElbw9eIVrc9ZTY=',
}

headers = {
    'authority': 'm.tiktok.com',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
    'sec-ch-ua-platform': '"macOS"',
    'accept': '*/*',
    'origin': 'https://www.tiktok.com',
    'sec-fetch-site': 'same-site',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://www.tiktok.com/',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
}

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
