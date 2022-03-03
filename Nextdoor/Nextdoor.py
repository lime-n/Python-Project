from playwright.async_api import Response as PlaywrightResponse
from scrapy_playwright.page import PageCoroutine
from scrapy import Spider, Request, FormRequest
import jsonlines
import pandas as pd
from collections import defaultdict
import json

cookies = { "Your cookies here"}

headers = {"Your headers here"}

full_url_post = {
    'group_name' :['For Sale or free', 'Free furniture', 'Gardening', 'Recruitment - Job Search', 'Deals & Discounts', 'CAT LOVERS', 'Cleaner', 'Business Owners, Entrepreneurs & Small Businesses'],
    'urls' : ['https://nextdoor.co.uk/g/f1j03vs5p/?is=nav_bar', 'https://nextdoor.co.uk/g/rv2gy7olj/?is=nav_bar', 'https://nextdoor.co.uk/g/mgvsnzktt/?is=nav_bar',
        'https://nextdoor.co.uk/g/qbutu7via/?is=nav_bar', 'https://nextdoor.co.uk/g/a9fnmlnqz/?is=nav_bar', 'https://nextdoor.co.uk/g/gb9xl68su/?is=nav_bar', 
        'https://nextdoor.co.uk/g/l2noxsnto/?is=nav_bar', 'https://nextdoor.co.uk/g/qd9pskzfj/?is=nav_bar']
}


class DoorSpider(Spider):
    name = 'door'
    start_urls = ['https://nextdoor.co.uk/login/']

    custom_settings = {
        'DOWNLOAD_DELAY':0.5
    }

    """
    Create a login event into Nextdoor
    """
    def start_requests(self):
        for url in self.start_urls:
            yield Request(
                url=url, 
                headers=headers,
                cookies=cookies,
                callback = self.parse, 
                meta= dict(
                        playwright = True,
                        playwright_page_coroutines = [
                        #PageCoroutine("click", selector = ".onetrust-close-btn-handler.onetrust-close-btn-ui.banner-close-button.onetrust-lg.ot-close-icon"),
                        PageCoroutine("wait_for_timeout", 3000),
                        PageCoroutine("fill", "#id_email", 'your_email'),
                        PageCoroutine("fill", "#id_password", 'password'),
                        PageCoroutine("wait_for_load_state", "load"),
                        PageCoroutine("click", selector="#signin_button"),
                        PageCoroutine("wait_for_load_state", "load"),
                        PageCoroutine("wait_for_timeout", 9000),
                        PageCoroutine("screenshot", path="login.png", full_page=True),
                        
                                ]
                            ))

    def parse(self, response):
        """
        Create a page handler to specifically grab content of the context, such as their method, url and resource type. This is because we're scrolling -
        down the pages to grab the payload of each unique content. We use an infinite scroll and increase the event timer (Improvements in notes.)
        """
        for url_ in full_url_post['urls']:
            yield Request(url=url_,
            headers=headers, 
            cookies=cookies,
            meta= dict(
                        playwright = True,
                        playwright_page_coroutines = [
                        
                        PageCoroutine("wait_for_timeout", 5000),
                        #PageCoroutine("screenshot", path="page.png", full_page=True),
                        PageCoroutine("wait_for_function", """setInterval(function () {
                                    var scrollingElement = (document.scrollingElement || document.body);
                                    scrollingElement.scrollTop = scrollingElement.scrollHeight;
                                }, 200);"""),
                    PageCoroutine("wait_for_timeout", 80000000)],
                        playwright_page_event_handlers = {
                            "response":"handle_response"
                        },)
                            )

    async def handle_response(self, response: PlaywrightResponse) -> None:
        """
        Save the content from the networks of any web-browser specifically for `xhr` resource types.
        """
        self.logger.info(f'You have logged the following data: {response.request.method, response.request.url, response.request.resource_type}')
        jl_file = "next_door.jl"
        #data = defaultdict(list)
        data = {}
        if response.request.resource_type == "xhr":
            if response.request.method == "POST":
                if "groupsWithPromosFeed" in response.request.url:
                    data['resource_type']=response.request.resource_type,
                    data['request_url']=response.request.url,
                    data['post_data']=response.request.post_data_json,
                    data['method']=response.request.method
                    with jsonlines.open(jl_file, mode='a') as writer:
                        writer.write(data)
        else:
            pass

    def parse_url(self, response):
        """
        Parse the data for group_id and next_page unique keys. Then send requests to the site.
        """
        data_post = pd.read_json("next_door.jl", lines=True)
        test= defaultdict(list)
        for post in data_post.post_data.values:
            for request_post in post:
                #print(request_post)
                if ('groupId' in request_post['variables']['groupsFeedArgs'].keys()) and ('nextPage' in request_post['variables']['groupsFeedArgs'].keys()):
                    test['groupid'].append(request_post['variables']['groupsFeedArgs']['groupId'])
                    test['nextpg'].append(request_post['variables']['groupsFeedArgs']['nextPage'])
        for groupid, nextpg in zip(test['groupid'], test['nextpd']):
            yield FormRequest(
                url = "https://nextdoor.co.uk/api/gql/groupsWithPromosFeed?",
                formdata = '{"operationName":"groupsWithPromosFeed","variables":{"pagedCommentsMode":"FEED","facepileArgs":{"variant":"TRIMMED"},"groupsFeedArgs":{"groupId":"%s","feedPageSourceType":"GROUP_DIRECTORY_PAGE_TYPE","feedRequestId":"83B55CCF-9184-40F4-A009-15709EA83B34","nextPage":"%s"},"timeZone":"Europe/London","nuxStates":["GOOD_NEIGHBOR_PLEDGE"]},"extensions":{"persistedQuery":{"version":1,"sha256Hash":"ce1950c657785ea25a2c59cc29ecf9b5e4121c79f16b92231b175edabff87f1f"}}}' % (groupid, nextpg),
                header=headers,
                cookies=cookies,
                callback = self.parse_slug,
                
                )

    def parse_slug(self, response):
        """
        The objective here is to grab the `slug` values from each API request. By grabbing a large enough number of unique `slug`, which is the location name -
        used by the Nextdoor to get Hood info for each neighbourhood.
        """
        slug_data = defaultdict(list)
        for slug in json.loads(response.text)['data']['me']['groupsWithPromosFeed']['feedItems']:
            slug_data['slug'].append(slug['post']['author']['originationNeighborhood']['slug'])
            slug_data['shortName'].append(slug['post']['author']['originationNeighborhood']['shortName'])
            slug_data['city'].append(slug['post']['author']['originationNeighborhood']['city'])
            slug_data['state'].append(slug['post']['author']['originationNeighborhood']['state'])
            slug_data['group_title'].append(slug['post']['audience']['link']['title'])
        yield {'data':slug_data}
            
        """
        ...
        The hardest part has been accomplished. To get the rest of neighbourhood information we have to redirect to the neighbourhood url, then send requests - 
        to that url by using our `slug` values to grab content for each neighbourhood. This will require an infinite scroll to grab all the posts for each neighbourhood.
        
        """
        

