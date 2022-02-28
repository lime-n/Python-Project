# Python-Scrapy-Project

This folder will contain scripts coded in the Python-programming language. These are web-scraping techniques that involved the following skills:
- Understanding of Scrapy networking for Requests
- Combining BeautifulSoup and JSON to parse web-page data within Scrapy for complex websites
- Practice and use of ItemLoaders, cb_kwargs, meta, Pipelines, Settings and Items.
- The implementation of Scrapy Playwright for Javascript loading pages; i.e. filling forms on a page, clicking buttons.
- The implementation of Scrapy Splash for Javascript loading pages; i.e. filling forms on a page, clicking buttons.
- Interacting with the JSON data from a webpage (Networks tab)
- An automator in selenium with multithreading to auto-click and auto-fill aspects of a page.

I have built a scraper to optimally parse Instagram with SplashRequest/Scrapy.Request for optimum parsing speeds.

Additional skills I have learnt:
- Implementation of lua with splash
- Implementation of Javascript with splash
- Alternative proxies and User-agents to prevent IP ban.

## Projects in Progress:
- Scrape the Nextdoor App webpage
  - ~~I have a scraper to fill form details to login and access the website~~
  - ~~Need to find a way to imitate user activity because requests to the network tab are dynamic~~
    -Successfully completed the Nextdoor scraper -Contact me for details on the Nextdoor scraper and data availability: emiljan_mrizaj@icloud.com

- Archive: scraping book pages from sources not accessible freely to the public elsewhere. To combine these book pages into pdfs and share these with free access.

## Scrapy_playwight-Projects

This will include project scripts that work with the `scrapy_playwright` framework.

Recent projects:
- Have only specific response types sent to the browser request
- ~~Download the response urls, type and methods. Furthermore, grab the payload information for POST requests without human interaction on the browser.~~
- Completely automate with the browser via `scrapy_playwright` and `scrapy` integration without user interaction on the web-browser.
  - To achieve this I need to send specific response types to the browser request. Primarily not to overload the server, or my CPU. Then extract the url types by method - save the output through the `playwright_page_event_handler`, and parse the links on the next method.
