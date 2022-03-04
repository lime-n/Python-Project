Still work in progress:

- ~~I have accomplished form filling the details to enter the site.~~
- ~~Found out where the JSON containing all the details for every neighbourhood~~

Requirements:  
- ~~Waiting for the developers to answer the following suggestion posted on their [github](https://github.com/scrapy-plugins/scrapy-playwright/issues/61)~~
  - Successfully completed the Scraper.

The following file: `Netdoor.py` contains the script required to properly scrape the Nextdoor whilst using only Scrapy and Scrapy plugins. I have indented additional notes in the script, however further information is detailed here:

**Notes:**
- We first grab a selection of url that direct to neighbourhood groups. From here, we can grab a number of diverse neighbourhood locations that is required to send into the payload to grab data from multiple neighbourhoods.
- By collecting this information, namely as Nextdoor names it - 'slug', then we can scrape the official neighbourhood information containing all data the Nextdoor provides while in Json format.  

Improvements:
  - To build a Javascript infinite scroll with a mouse-scroll timer to approximately ~1 second lag time between each scroll. This prevents the server from overloading, and to give scrapy enough time to collect information on the site.
  - An alternative measure to `wait_for_timeout`, such that when you have reached the botton of the page, then close that page. This should save the CPU from overloading because multiple pages are open.
