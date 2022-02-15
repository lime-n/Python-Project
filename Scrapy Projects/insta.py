import scrapy
from scrapy_splash import SplashRequest


script = """
function main(splash)
    assert(splash:go(splash.args.url))
    local get_dimensions = splash:jsfunc([[
        function () {
            var rect = document.querySelector("button:nth-child(2)").getClientRects()[0];
            return {"x": rect.left, "y": rect.top}
        }
    ]])
    splash:set_viewport_full()
    splash:wait(0.3)
    local dimensions = get_dimensions()
    -- FIXME: button must be inside a viewport
    splash:mouse_click(dimensions.x, dimensions.y)
    -- Wait split second to allow event to propagate.
    splash:wait(6)
    local send_keys_again = splash:jsfunc([[
    function() {
    const setValue = Object.getOwnPropertyDescriptor(
    window.HTMLInputElement.prototype,
    "value"
    ).set
    const modifyInput = (name, value) => {
    const input = document.getElementsByName(name)[0]
    setValue.call(input, value)
    input.dispatchEvent(new Event('input', { bubbles: true}))
    }

    modifyInput('username', "novelife@outlook.com")
    modifyInput('password', "Mrizoj.2")
    document.querySelector("button[type='submit']").click()
    }
    ]])
    splash:wait(0.3)
    send_keys_again()
    -- FIXME: button must be inside a viewport
    --splash:mouse_click(submitform.x, submitform.y)
    splash:wait(10)
    

    return splash:html()
end
"""

class InstaSpider(scrapy.Spider):
    name = 'insta'
    allowed_domains = ['instagram.com']
    start_urls = ['http://instagram.com/']

    custom_settings = {
        'USER_AGENT':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
    }

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(
                url = url, 
                callback = self.parse, 
                endpoint='execute',
                args={'lua_source':script}
            )

    def parse(self, response):
        yield response.follow(
            url = "https://www.instagram.com/explore/tags/tesla/",
            callback = self.parse_data
        )
    def parse_data(self, response):
        ''' 
        parse the data here with scrapy for optimum speed 
        '''

        yield {
            'data':response.text
        }
