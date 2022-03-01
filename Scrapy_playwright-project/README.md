
**Scrapy Playwright Projects:**

A compilation of my project scripts for `playwright` integration with `scrapy` are uploaded here.

**Methodology:**

1. Infinite Scroll
- This project continuously scrolls down the tiktok page for short-clips involving the tesla tag.
- The aim was to fetch the api data from the networks tab without having to enter it. Furthermore, tiktok make this more difficult to do manually as the payload to the api is unique on each scroll. Therefore, you have to scroll to collect it.
- The script automates the scroll and grabs the `FETCH` resource type which contains the `API` for tiktok and the `payload` in the url. Therefore, saving the results and reading them in the next method for further parsing.

Additional notes:
- Opportunity to grab various results more multiple resource types for dynamic scrolling sites that have a defense towards the payload extraction. Such as the example above, where you would otherwise have to manually get the payload each time to access the JSON data.
- I need to figure out a way to infinitely increase the timer without `wait_for_timeout`, this is unsuccessful for one other website that I've tried this on.
 - There's now a way to abort downloaded requests from getting responses to resource types.
