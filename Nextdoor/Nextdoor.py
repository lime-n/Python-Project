from playwright.async_api import Response as PlaywrightResponse
from scrapy_playwright.page import PageCoroutine
from scrapy import Spider, Request, FormRequest
import jsonlines
import pandas as pd
from collections import defaultdict
import json

cookies = {
    'WE': 'df14c814-2462-4659-a7f6-ed8ea5f604ee220227',
    'G_ENABLED_IDPS': 'google',
    '_gcl_au': '1.1.989813006.1645995147',
    '_rdt_uuid': '1645995147329.c6c0ba12-af24-4a13-9f19-7e997c0ed4a2',
    '_gid': 'GA1.3.934130007.1645995147',
    '_scid': '992921e5-03a5-4e94-b51d-9c18ee9554fe',
    '_fbp': 'fb.2.1645995147668.1931482921',
    '__pdst': 'de10943d3ff341b58f2ee8da3b7daaec',
    'hubspotutk': '8fa1044e3612c0c7d36d3b5ef7044ed0',
    '__gads': 'ID=c8c2166664b84ecc:T=1646064334:S=ALNI_MZ6knqEJ8iH51w1AnOX_ycBt08nJw',
    'OptanonAlertBoxClosed': '2022-02-28T16:05:35.302Z',
    '_pin_unauth': 'dWlkPVpqWTVOMlZsTTJVdFlUbGlaUzAwT1RGbExUaG1NR010WmpZMVkyVXhNalk0WkdZeg',
    'lang': 'en-gb',
    '_clck': '1d62ltu|1|eze|0',
    'role-invitation-decline-notification': 'false',
    'role-resignation-notification': 'false',
    'seen-tour-in-past-day': 'true',
    '__hssrc': '1',
    'csrftoken': 'ZMnskqdzf6zq5dI7SjEIzQ755dmGrYOYBjgzViGgNBHa3ENxVufy948ZSRMryNXx',
    'WERC': '1b1e0062-bcc4-4f27-946b-58783c6f64332203011646161950',
    'ndbr_at': '4-0hOJQpUUTwEC4zrVvQZxdHZN17rDXHvS_YWHKFqck',
    'ndbr_idt': 'eyJhbGciOiJSUzI1NiIsImtpZCI6ImFlNjVkMTc1LTRiODYtNGQ0OS04NDgzLWJiMjE3MDMwODY5YSIsInR5cCI6IkpXVCJ9.eyJhdF9oYXNoIjoiNmZrZEEwS0xLWWp1Tnd2djVHYVpsdyIsImF1ZCI6WyJuZXh0ZG9vci1kamFuZ28iLCJuZXh0ZG9vciJdLCJleHAiOjE2NDYyNDgzNTAsImlhdCI6MTY0NjE2MTk1MCwiaXNzIjoiaHR0cHM6Ly9hdXRoLm5leHRkb29yLmNvbSIsIm5kX3ByYyI6W3siY291bnRyeSI6IkdCIiwicGlkIjoiMTc1OTIyMDAyODI0NzAiLCJ1cmwiOiJodHRwczovL3VzZXIubmV4dGRvb3IuY28udWsvdjEvcHJvZmlsZS8xNzU5MjIwMDI4MjQ3MCJ9XSwic3ViIjoiZDdhZWZjMmMtYTUyMy00YWJkLWI0YTEtZTRmMDBjMzc4ZjAyIn0.NB7iPdm_GHH4UICVOtMSNC4Xk1uhMC81t6OkZer_aYi_MlI-EoDpY4M-zJBPLVU-WOQtYOR1fALvJp2tyZf1GLw3PAuhNd4BfTiQA5kbOiZ_h68E79YQdcUh0GAJLBhsVrNuXcQL0w5lPr09S7OMXSgwtTiuc9h_M83iYj4gwdY63n7jVa94b9am-dQh914GzmeC2ErUM64-KKzznhP9Cf_KkxXLTdhQ2p1kL-MuBLi3rILACAiP-BQmE5D8wQd1-h6KcyZw_470F8kNYrhI6KJCJCQNb7iCk7-9Fix4pR8CzmM5hP7IrPShMLZXCV1UFNvpG__20gENda5XTbOnEw',
    's': 'tup88te7ncrvoungh8rfjrcpynktylr4',
    'OptanonConsent': 'isIABGlobal=false&datestamp=Tue+Mar+01+2022+19%3A12%3A31+GMT%2B0000+(Greenwich+Mean+Time)&version=6.10.0&hosts=&consentId=f695093c-842f-40f0-8bc5-2f62f9c8fab1&interactionCount=2&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A0%2CC0004%3A0%2CC0002%3A0&AwaitingReconsent=false&geolocation=GB%3BENG',
    '_gat_UA-18585915-10': '1',
    '_gat_UA-18585915-1': '1',
    '_gat_UA-18585915-20': '1',
    '_uetsid': '2bc82420980f11ecb4f847571fa44ae9',
    '_uetvid': '2bc84a30980f11ec8946d35cebe96f10',
    '_ga': 'GA1.1.1925607416.1645995147',
    '__hstc': '102379230.8fa1044e3612c0c7d36d3b5ef7044ed0.1645995149280.1646159686318.1646161952758.10',
    '__hssc': '102379230.1.1646161952758',
    '_clsk': 'rm68ze|1646161957479|3|0|f.clarity.ms/collect',
    '_ga_L2ES4MTTT0': 'GS1.1.1646161950.15.1.1646161957.53',
    'AWSALB': 'rWo4khogRoHwAC3JiyJKJcD7qMdn/yMGYyf0gFw+QiPNegZ74wOiev6IVSr2Qnkq5kAjLlM/XI4SB0ZpMzlHy8H63WaSehFQvhEM18JKUpGZgKsSf4ZT6TTHoRrOXBCoND6PYUYyIZqSKzRTCgAPLTqqXPaXtr8DjJx4aVr/ssSs5Jv4HA5qDJReragkcA==',
    'AWSALBCORS': 'rWo4khogRoHwAC3JiyJKJcD7qMdn/yMGYyf0gFw+QiPNegZ74wOiev6IVSr2Qnkq5kAjLlM/XI4SB0ZpMzlHy8H63WaSehFQvhEM18JKUpGZgKsSf4ZT6TTHoRrOXBCoND6PYUYyIZqSKzRTCgAPLTqqXPaXtr8DjJx4aVr/ssSs5Jv4HA5qDJReragkcA==',
    '_dd_s': 'logs=1&id=74adb7ad-c0bd-4575-acda-8d307e5a50b9&created=1646161942846&expire=1646162864028',
}

headers = {
    'authority': 'nextdoor.co.uk',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'sec-ch-ua-mobile': '?0',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36',
    'content-type': 'application/json',
    'accept': '*/*',
    'x-nd-activity-source': 'no-referrer',
    'x-nd-activity-id': 'BD10344F-80C3-4126-8AE2-B52E81DF7108',
    'x-csrftoken': 'ZMnskqdzf6zq5dI7SjEIzQ755dmGrYOYBjgzViGgNBHa3ENxVufy948ZSRMryNXx',
    'sec-ch-ua-platform': '"macOS"',
    'origin': 'https://nextdoor.co.uk',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://nextdoor.co.uk/g/rv2gy7olj/?is=nav_bar',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
}

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
        

