import hashlib
from pathlib import Path
from scrapy.crawler import CrawlerProcess
import scrapy
from scrapy_playwright.page import PageCoroutine
from scrapy.http.response import Response
from pathlib import Path
from typing import Generator, Optional
from bs4 import BeautifulSoup
import requests
import pandas as pd
from collections import defaultdict

headers1 = {'authority': 'www.jobsite.co.uk',
'method': 'POST',
'path': '/Account/AccountDetails/GetUsersEmailAddress',
'scheme': 'https',
'accept': '*/*',
'accept-encoding': 'gzip, deflate, br',
'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
'content-length': '0',
'cookie': 'VISITOR_ID=3c553849d1dc612f60515f04d9316813; INEU=1; PJBJOBSEEKER=1; LOCATIONJOBTYPEID=3079; AnonymousUser=MemberId=c3e94bc3-3fcf-423d-bc25-e5a5818cd2b9&IsAnonymous=True; visitorid=46315422-d835-4a38-b428-4b9c5d6243d3; s_fid=6468EA5E39AF374B-2F7C971BB196D965; sc_vid=7c12948068d2cb92c1f1622aeaabc62d; bm_sz=040B815C79F42322C6F55B9A84059612~YAAQkjMHYIT0S/19AQAAdntxMA5K8hYQ/xEiGMe5kfWsY2+92eOVpwnewEY3vnLuUAjKAoVn2uSc2MzFaqBHPm0WC+sc06cnqNlX38HRuIOFGmX9bGf1X19+IWQ9fmRcCpTgdEgVQXn5I+JxiWbVQQ/b+BDWc/7sO4SSp4dohaK4HtUBBn9dM816ldpcebE6wARHqJ1/rU2D7CQWDlwZKElVWpckfSqbkRAGZU2LcR236v7y7VvEgQXDbLZt1+hK1XQdwGOk/IqCUJ/S8tN5+bv61EoN48XNoa59wW1nXXJ/Nt7dJ6k=~4408632~4277301; listing_page__qualtrics=run; SsaSessionCookie=ab1f18f8-ec5d-4009-a914-95d2f882a97f; s_cc=true; SessionCookie=e6f10e4e-d580-4080-b16e-4a0005e1250b; bm_mi=02373D7983A008BAA61DFB006C1C8A0E~QKtA+MDyql6ENWQY8PRxd7Pqtaufphkw420ior2myH3DZ6Ae19lIXWpswSDF1rxxpCOMw1A/Xj42mDE45UQBoYGNtM/WLe8kQUicaFQ14Ins6++/b8PJ4xOJdMAzc/JImoYfkrrTk4S32JJvvPvneHe14i0EboPJo0IfpWuVuAPK4z0pY8kP2jAi0AkWvLF1G0QNYSU48wpAXe3WmnZOTGnP4zqowbVMiZiFt4sf5yR3CZSS9wV2TDNedylo0bieo1FUG7zqZiEbO/mFsWcRBI1180TWS6BGAAbBEZvxyw0HnlqrsPIp5quduoSS8Ehd; gpv_pn=%2FJobSearch%2FResults.aspx; s_sq=%5B%5BB%5D%5D; bm_sv=F6E62687D7A0F5C67D3AF1CABD6B9744~fDgputVUji95V4kU8PeHKlB8aj5CZ3qqhrtnsQRccZT2TKTMz/FUh5DwGwGO5oL7Y3wv0jsshnBru8dpe3/nUrtBcQ+14jM0dn5AqDN6LrxRfmTkiwnAcczsZZH2/Xj2fVroEyjGwc6FRdbCXZYL8Kvie1so2TY11EOz62OtcLw=; ak_bmsc=23E489A2DC2C230BB1D33B05DBA9BFEB~000000000000000000000000000000~YAAQ1DMHYDj7Z+19AQAAuhiDMA5vZLtn2bkBpZKSIYdvX1kjNcbAGcvQkRrju3gAjN4luOn2/AXFtXR4Vf23GoBabEa0H6ti6nj7K5wYDnaRL7IxtdNQiXNeh9Y4JSPFC8qoJ28QZu4CgEuYtITCS31GhjHOkORGy4//xNOE9SgwfqJ6OYLOV7ynQ4xrMGzo9Z62kCq48cIOkex/jCf1acEm56q1c6iY0qV/ide37GOWm+iAE/Y6irsdg6u+h3ZnF4r6fSFLhVmAL8jRN4j4m4QcY8O6E+L88V5r9cWDxHqviyBpuq2MIVT/UUmJmveSusPdJaHSykqozx1n8MciFQDT4YVoFv26ZzzLP6UPHHmGQVUAMBejmVp4Z4Rku5uzWpmJqunufp0oXBbj8VO90NzhAjxrVbkcmUN4fsdCBPi1a/E3GY4WHoVy1IFAOq1JIFReOfrK9oodOkAlNLvEmw==; FreshUserTemp=https://www.jobsite.co.uk/jobs/degree-mathematics?s=header; TJG-Engage=1; CONSENTMGR=c1:0%7Cc2:0%7Cc3:0%7Cc4:0%7Cc5:0%7Cc6:0%7Cc7:0%7Cc8:0%7Cc9:1%7Cc10:0%7Cc11:0%7Cc12:0%7Cc13:0%7Cc14:0%7Cc15:0%7Cts:1641492245529%7Cconsent:true; utag_main=v_id:017e24e57d970023786b817ac51005079001e071009e2$_sn:19$_se:21$_ss:0$_st:1641494045530$ses_id:1641490250874%3Bexp-session$_pn:11%3Bexp-session$PersistedFreshUserValue:0.1%3Bexp-session$PersistedClusterId:OTHER--9999%3Bexp-session; s_ppvl=%2FJobSearch%2FResults.aspx%2C13%2C13%2C743%2C1600%2C741%2C1600%2C900%2C2%2CP; s_ppv=%2FJobSearch%2FResults.aspx%2C43%2C13%2C2545%2C1105%2C741%2C1600%2C900%2C2%2CP; EntryUrl=/jobs/Degree-Accounting-and-Finance?s=header; SearchResults="96078524,96081944,96081940,96081932,96028790,96023400,95997230,95988474,95911593,95886582,95822047,95794932,95717150,96049263,96104930,96103354,96097166,96094806,96103713,96104313,96092943,96078775,96091168,96097009,96106218"; _abck=508823E0A454CEF8D6A48101DB66BDB8~-1~YAAQZIlJF/Qx/rR9AQAA6CaQMAcuV5W+q0rR81SHANopxLna+xREkvs/PgVa+4GQrKei8nvkruKjFI1ij6jDgOcLUe+NwnbBvvnhhAf3QYmHwP9plWN11rY4azCfJX98LZ8WTUvDj3qai57Y/xVKGQtvm8/rDrZODEUq4HM1iieSnIopTWGQv5UQYUbPr/IIyi3ajN+BCywOEhCk/1TqDAdQpEmfdwAyyMm2dae0UrPYPL9TtQhJR5md0vudL6ViRDANKpm3XhiWQ/gQzNEVhzvTufprBqOfvqWYImvZtByld5i8Szqhffs78xg2LE4FE4dXeZDEz5/DlV0S3KUIZdTU/tpwqDzTMECOKiG6EgR5s2YQxgwpbbzFRFJ0N8tjHO2VcMzmyfHH7pOfoW6acVh+Ww5skZbN1kB8fA==~-1~||1-IIljDRDehm-1-10-1000-2||~-1',
'origin': 'https://www.jobsite.co.uk',
'referer': 'https://www.jobsite.co.uk/jobs/Degree-Accounting-and-Finance?s=header',
'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
'sec-ch-ua-mobile': '?0',
'sec-ch-ua-platform': '"macOS"',
'sec-fetch-dest': 'empty',
'sec-fetch-mode': 'cors',
'sec-fetch-site': 'same-origin',
'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36x-requested-with: XMLHttpRequest'
          }
cookies = {
    'VISITOR_ID': '3c553849d1dc612f60515f04d9316813',
    'INEU': '1',
    'PJBJOBSEEKER': '1',
    'LOCATIONJOBTYPEID': '3079',
    'AnonymousUser': 'MemberId=c3e94bc3-3fcf-423d-bc25-e5a5818cd2b9&IsAnonymous=True',
    'visitorid': '46315422-d835-4a38-b428-4b9c5d6243d3',
    's_fid': '6468EA5E39AF374B-2F7C971BB196D965',
    'sc_vid': '7c12948068d2cb92c1f1622aeaabc62d',
    'listing_page__qualtrics': 'empty',
    'SsaSessionCookie': 'fea6276c-cee9-43b3-9d57-9f00d6bcd32b',
    's_cc': 'true',
    'SessionCookie': '1d7806bc-4b79-47e9-839d-2d94ec224abb',
    'FreshUserTemp': 'https://www.jobsite.co.uk/',
    'bm_mi': '6BF6AA183A047F87BAC664C92ACA8E41~1Fku4TDwEBxz2+fwhUGUWjUhP3vaQED08Ala3VmmARyewb9/OjQUmvPEWw88MUA7USOzt+0MSpdyPmY/3N+iY08InyOy4DnNHgTq88AWwBigf1XhufLstD/eUhUJBgXQRSa1rVlO5SB5mlkhezcDRmv8bL+Gt4NZdsjVC4ZlVc3ptkbKY9cBB65yW2tyjZBLtxsQnz/rFJXo4a9PTKOvF/Betnb8S/XQrpNDsXOojdhtQrrU9V6XSziX+tHXT6xj1osB8XQtm0VGC7L6+4+bgQ==',
    'gpv_pn': '%2FJobSearch%2FResults.aspx',
    'ak_bmsc': '77506B6768E0463D238EEE24AE5B3A72~000000000000000000000000000000~YAAQFsITAnaZtJ99AQAA4LdCLw4PU4xFjE3/FbxxIG7pSjNqX9TClutWaS1MLKKy/9hAM9d6bcEN5Mr9Fbb8+1Jy3rrCsFO5TvxstcVAjaGbbvDCF/mXxeqJQAU1h/cvrZEH68FZyDuslnE+Ae7DuCs1QmNkNP6+0dvA4GT+/MENayQQk8szCo8ch3IfCK1j5/JL+jjbb04pmnpibV3XvUcLeqTJMY1IG9PlTuBIFWF8gXREI+ug2bb8pL+r7T1v1s9gVmfo633B0BoVcXIfWcDgtyFJjFNVayz2lHxUdtnInaWvi1ubzsjQ7cfUDdHTorHsJ0rP1RXB0utZ80GIBNbGdAzd1jkWy9BMIqdIcbBXM4+rCf3fbPw+qui+0Sr4RIxM5N41mvrOQ6W8s9bPR7GySeJr/2HGSmxTjf+4QDVY',
    'TJG-Engage': '1',
    'CONSENTMGR': 'c1:0%7Cc2:0%7Cc3:0%7Cc4:0%7Cc5:0%7Cc6:0%7Cc7:0%7Cc8:0%7Cc9:1%7Cc10:0%7Cc11:0%7Cc12:0%7Cc13:0%7Cc14:0%7Cc15:0%7Cts:1641470409597%7Cconsent:true',
    'utag_main': 'v_id:017e24e57d970023786b817ac51005079001e071009e2_sn:16$_se:5$_ss:0$_st:1641472209641$ses_id:1641470390747%3Bexp-session$_pn:3%3Bexp-session$PersistedFreshUserValue:0.1%3Bexp-session$PersistedClusterId:OTHER--9999%3Bexp-session',
    's_ppvl': '%2FJobSearch%2FResults.aspx%2C13%2C13%2C741%2C409%2C741%2C1600%2C900%2C2%2CL',
    's_ppv': '%2FJobSearch%2FResults.aspx%2C100%2C13%2C6616%2C423%2C741%2C1600%2C900%2C2%2CL',
    's_sq': 'stepstone-jobsite-uk%3D%2526c.%2526a.%2526activitymap.%2526page%253D%25252FJobSearch%25252FResults.aspx%2526link%253DNext%2526region%253Dapp-unifiedResultlist-db2486f4-fb7d-469f-8cfd-f31a3eafb692%2526pageIDType%253D1%2526.activitymap%2526.a%2526.c%2526pid%253D%25252FJobSearch%25252FResults.aspx%2526pidt%253D1%2526oid%253Dhttps%25253A%25252F%25252Fwww.jobsite.co.uk%25252Fjobs%25253Fpage%25253D3%252526action%25253Dpaging_next%2526ot%253DA',
    'bm_sv': '4C178898519D2A4ADEBB840C0B682999~sanqWSDI/ZT0KWrdWhNRc7UtVtqAZ61oPSoLv/MnCD1e0a7vUTSzpggIj9dt/bN4nXEmOaM48hugBFRwdBveJlobrjEcMZ1gHS3S3KXYaHfZPjq6IIf8/Fs1QUlg0s7oLp6DsZbkAWWOnNQiI/uaq7XT7EHnd+n/46ra5jgwfhA=',
    '_abck': '508823E0A454CEF8D6A48101DB66BDB8~0~YAAQFsITAjWdtJ99AQAAr21ELwcAj2rhoIgnvsHOkxQPuREHCA9mDMHsyk68FBhxQ0Jto+6FqaEHJkrrVEUGuYveQAjVJ7CGS+2ajmbcVkG/KIQn8ttCaGvn58jkwzpWm6Fjx4FsLBJyLsceRWSqw5rV2ezEeLrBd/ZToRMpdZop4yqixh5vquandn+h9ysqacaeHPO90VnvctIfvKTUvY5GrrHubGVMkD9/elxRI5whsBdH7ovATyGsLEgYx+e604lY2sQIahSvweclTI4Ud1hTQbSQTebWs52PiYdSU5wq9+YC/7Sr0JuQZCUMyGGqZgtXpfAdc9LDa8X3JfcdO25EZQHxsfEfT/pp7tjbxaXD/pgun9ozymRMy/hBuCj5/Bfln/LzAqOsdDv7q6WVerNr6qivHGDE0m2/~-1~-1~-1',
    'bm_sz': '747D15CEB59AC2C0003BD8479C4BF482~YAAQFsITAjadtJ99AQAAr21ELw5vDH+lMq9NICfxNXHGiXcPcBSrWov2Hy8Y0wgN/OAL7NJWfJ7Lkum/OqG3WNj9/+e8oJhNRQ96ksn+zk0N0gNnoPhUv46am0wktHih1PPfYRlqdPSQSdgE92eHwG3CsFaSeRROKu/1q89aNDH4+JBUk/TDdTmeBqsvJffzvP0S1gAv54dOecx0z2LSW6PEj0e0VtWqmBjFSQxCqH8LZ4r7TwqDpxAKArzWGDMlqR/xZcWvAm8ijUTG+mIuF3N7aBEDGdB90wdyaJGt2CGP3VinhBNtxV7vT8ebY9oWu2rJ+UmugGgJ/dasQP8=~3424837~3354936',
    'EntryUrl': '/jobs?page=1&action=paging_next',
    'SearchResults': '96094529,96094530,96094528,96094527,96094526,96094525,96094524,96094522,96094521,96094520,96094519,96094517,96094518,96094514,96094513,96094509,96094510,96094511,96094508,96094507,96094506,96094503,96094502,96094500,96094499',
}

class JobSpider(scrapy.Spider):
    name = 'job_play'

    start_urls = defaultdict(list)
    
    #html = f'https://www.jobsite.co.uk/jobs/{}' We implement this if it works

    custom_settings = {
        'USER_AGENT':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15',
        

    }
    def start_requests(self):
        degree_data = pd.read_csv('/Users/emiljanmrizaj/indeed/indeed/degree_names2.csv')
        links = defaultdict(list)
        for query, title in zip(degree_data.degrees, degree_data.degree_type):
            url = f'https://www.jobsite.co.uk/jobs/{query}'
            r = requests.get(url, headers = headers1)
            soup = BeautifulSoup(r.content, 'lxml')
            pages = soup.select('.PageLink-sc-1v4g7my-0.gwcKwa[data-at]')
            test = []
            for num_pages in pages:
                test.append(num_pages.text.strip())
            for pages in [test[1::2]]:
                if pages == []:
                    links[query].append(1)
                elif pages is not []:
                    for i in pages:
                        links[query].append(i)
        for query, values in links.items():
            for val in values:
                for pages in range(1, int(val)+1):
                    self.start_urls[query].append(f'https://www.jobsite.co.uk/jobs/{query}?page={pages}.html')
        for category,urls in self.start_urls.items():
            for link in urls:
                yield scrapy.FormRequest(
                    url = link,
                    cookies=cookies,
                    callback = self.parse,
                    meta= dict(
                            playwright = True,
                            playwright_include_page = True,
                            playwright_page_coroutines = [
                                PageCoroutine('wait_for_selector', 'div.row.job-results-row')
                                ]
                        ),
                    cb_kwargs = {
                            'category':category
                        }
            )
    def parse(self, response, category):
        for jobs in response.xpath("//a[contains(@class, 'sc-fzqBkg')][last()]"):
            yield response.follow(
                jobs,
                cookies=cookies,
                callback=self.parse_jobs,
                meta= dict(
                    playwright = True,
                    playwright_include_page = True,
                    playwright_page_coroutines = [
                        PageCoroutine('wait_for_selector', '.container.job-content')
                        ]
                ),
                cb_kwargs = {
                    'category':category
                }
            )

    async def parse_jobs(self, response, category):
        yield {
            "category": category,
            "url": response.url,
            "title": response.xpath("//h1[@class='brand-font']//text()").get(),
            "price": response.xpath("//li[@class='salary icon']//div//text()").get(),
            "organisation": response.xpath("//a[@id='companyJobsLink']//text()").get()
        }
if __name__ == "__main__":
    process = CrawlerProcess(
        settings={
            "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
            "DOWNLOAD_HANDLERS": {
                "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
                "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            },
            "CONCURRENT_REQUESTS": 32,
            "FEED_URI":'jobs_test2.jl',
            "FEED_FORMAT":'jsonlines',
        }
    )
    process.crawl(JobSpider)
    process.start()
