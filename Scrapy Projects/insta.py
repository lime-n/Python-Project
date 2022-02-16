import scrapy
from scrapy_splash import SplashRequest
from scrapy.item import Field
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose, Join

headers = {
    'authority': 'i.instagram.com',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'x-ig-app-id': '936619743392459',
    'x-ig-www-claim': 'hmac.AR0gfAoxn0n5H5ozNiDsStHhcpQQ23cy2XVSjmySnxyMZTPl',
    'sec-ch-ua-mobile': '?0',
    'x-instagram-ajax': '09ee8eb6a113',
    'content-type': 'application/x-www-form-urlencoded',
    'accept': '*/*',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'x-asbd-id': '198387',
    'x-csrftoken': 'JqW7LVgKDaayZGRkeMe9of8D8PVeUMGz',
    'sec-ch-ua-platform': '"macOS"',
    'origin': 'https://www.instagram.com',
    'sec-fetch-site': 'same-site',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://www.instagram.com/',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    #'cookie': 'mid=YgrKTgAEAAEyDsCCvVtvKTR_x2DI; ig_did=344C0E32-AA3D-4008-99A4-28AA25C1280E; ds_user_id=7879922003; csrftoken=JqW7LVgKDaayZGRkeMe9of8D8PVeUMGz; sessionid=7879922003%3AfZfegYaDVxe5Og%3A16; rur="RVA\\0547879922003\\0541676496308:01f74391f0de130eea6b5752fde44d2ee100d6d9c20f655ff64106c4760cba3eb0db40c0"',
}

cookies = {
    'mid': 'YgrKTgAEAAEyDsCCvVtvKTR_x2DI',
    'ig_did': '344C0E32-AA3D-4008-99A4-28AA25C1280E',
    'ds_user_id': '7879922003',
    'csrftoken': 'JqW7LVgKDaayZGRkeMe9of8D8PVeUMGz',
    'sessionid': '7879922003%3AfZfegYaDVxe5Og%3A16',
    'rur': 'RVA,7879922003,1676496308:01f74391f0de130eea6b5752fde44d2ee100d6d9c20f655ff64106c4760cba3eb0db40c0',
}


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
class InstaItem(scrapy.Item):
    created_at = Field(output_processor = TakeFirst())
    text = Field(input_processor = MapCompose(str.strip),output_processor = Join())
    full_name = Field(output_processor = TakeFirst())
    username = Field(output_processor = TakeFirst())
    profile_pic_url = Field(output_processor = TakeFirst())
    has_anonymous_profile_picture = Field(output_processor = TakeFirst())
    is_private = Field(output_processor = TakeFirst())
    comment_count = Field(output_processor = TakeFirst())
    commericality_status = Field(output_processor = TakeFirst())
    like_count = Field(output_processor = TakeFirst())
    video_duration = Field(output_processor = TakeFirst())
    video_view_count = Field(output_processor = TakeFirst())
    latatitude = Field(output_processor = TakeFirst())
    longitude = Field(output_processor = TakeFirst())
    address = Field(output_processor = TakeFirst())
    city = Field(output_processor = TakeFirst())
    name_of_location = Field(output_processor = TakeFirst())
    carousel_media_count= Field(output_processor = TakeFirst())


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
        id_val = ['QVFBYlFuX0FGWkFTS2RtckwzRi1GVUdRaVNrOWprQjA4QUpfOHptMFgzcVR1RTdvaUxtVlpTRTVGOHNLSzJtbFhaMUZNbmVwS2pDbGlmaHBUamNuTFNLeA==']
        next_page = [1]
        for id, pages in zip(id_val, next_page):
            yield scrapy.FormRequest(
                url = "https://i.instagram.com/api/v1/tags/tesla/sections/",
                callback = self.parse_data,
                headers=headers,
                cookies=cookies,
                formdata = {
                        'include_persistent': '0',
                        'max_id': id,
                        'page': str(pages),
                        'surface': 'grid',
                        'tab': 'recent'
                                        },
                cb_kwargs = {
                    'id_value':id_val,
                    'next_page':next_page
                }
            )
                                            
    def parse_data(self, response, id_value, next_page):
        ''' 
        parse the data here with scrapy for optimum speed 
        '''

        data_id = response.json().get('next_max_id')
        data_page = response.json().get('next_page')
        all_data = response.json().get('sections')
        id_value = id_value + [data_id]
        next_page = next_page + [data_page]
        #loader = ItemLoader(InstaItem())
        for id, pages in zip(id_value, next_page):
            yield scrapy.FormRequest(
                url = "https://i.instagram.com/api/v1/tags/tesla/sections/",
                callback = self.parse_data,
                headers=headers,
                cookies=cookies,
                formdata = {
                        'include_persistent': '0',
                        'max_id': id,
                        'page': str(pages),
                        'surface': 'grid',
                        'tab': 'recent'
                                        },
                cb_kwargs = {
                    'id_value':id_value,
                    'next_page':next_page
                }
            )

        for i in range(0, len(all_data)):
            
            for first_tunnel in all_data[i]['layout_content']['medias']:
                loader = ItemLoader(InstaItem())
                if 'caption' in first_tunnel['media'].keys():
                    try:
                        if 'created_at' in first_tunnel['media']['caption'].keys():
                            loader.add_value('created_at',first_tunnel['media']['caption']['created_at'])
                    except:
                        continue
                else:
                    loader.add_value('created_at',"None")

                if 'caption' in first_tunnel['media'].keys():
                    if 'text' in first_tunnel['media']['caption'].keys():
                        loader.add_value('text',first_tunnel['media']['caption']['text'])
                else:
                    loader.add_value('text',"None")

                if 'caption' in first_tunnel['media'].keys():
                    if 'user' in first_tunnel['media']['caption'].keys():
                        if 'full_name' in first_tunnel['media']['caption']['user'].keys():
                            loader.add_value('full_name',first_tunnel['media']['caption']['user']['full_name'])
                else:
                    loader.add_value('full_name',"None")

                if 'caption' in first_tunnel['media'].keys():
                    if 'user' in first_tunnel['media']['caption'].keys():
                        if 'username' in first_tunnel['media']['caption']['user'].keys():
                            loader.add_value('username',first_tunnel['media']['caption']['user']['username'])
                else:
                    loader.add_value('username',"None")

                if 'caption' in first_tunnel['media'].keys():
                    if 'user' in first_tunnel['media']['caption'].keys():
                        if 'profile_pic_url' in first_tunnel['media']['caption']['user'].keys():
                            loader.add_value('profile_pic_url',first_tunnel['media']['caption']['user']['profile_pic_url'])
                else:
                    loader.add_value('profile_pic_url',"None")

                if 'caption' in first_tunnel['media'].keys():
                    if 'user' in first_tunnel['media']['caption'].keys():
                        if 'has_anonymous_profile_picture' in first_tunnel['media']['caption']['user']:
                            loader.add_value('has_anonymous_profile_picture',first_tunnel['media']['caption']['user']['has_anonymous_profile_picture'])
                else:
                    loader.add_value('has_anonymous_profile_picture',"None")

                if 'caption' in first_tunnel['media'].keys():
                    if 'user' in first_tunnel['media']['caption'].keys():
                        if 'is_private' in first_tunnel['media']['caption']['user'].keys():
                            loader.add_value('is_private',first_tunnel['media']['caption']['user']['is_private'])
                else:
                    loader.add_value('is_private',"None")
                
                if 'comment_count' in first_tunnel['media'].keys():
                    loader.add_value('comment_count',first_tunnel['media']['comment_count'])
                else:
                    loader.add_value('comment_count',"0")
                
                if 'commerciality_status' in first_tunnel['media'].keys():
                    loader.add_value('commericality_status',first_tunnel['media']['commerciality_status'])
                else:
                    loader.add_value('commericality_status',"None")
                
                if 'like_count' in first_tunnel['media'].keys():
                    loader.add_value('like_count',first_tunnel['media']['like_count'])
                else:
                    loader.add_value('like_count',0)

                if 'video_duration' in first_tunnel['media'].keys():
                    loader.add_value('video_duration',first_tunnel['media']['video_duration'])
                else:
                    loader.add_value('video_duration',0)

                if 'view_count' in first_tunnel['media'].keys():
                    loader.add_value('video_view_count',first_tunnel['media']['view_count'])
                else:
                    loader.add_value('video_view_count',0)
                
                if 'lat' in first_tunnel['media'].keys():
                    loader.add_value('latatitude',first_tunnel['media']['lat'])
                else:
                    loader.add_value('latatitude',"None")
                
                if 'lng' in first_tunnel['media'].keys():
                    loader.add_value('longitude',first_tunnel['media']['lng'])
                else:
                    loader.add_value('longitude',"None")
                
                if 'location' in first_tunnel['media'].keys():
                    loader.add_value('address',first_tunnel['media']['location']['address'])
                else:
                    loader.add_value('address',"None")
                
                if 'location' in first_tunnel['media'].keys():
                    loader.add_value('city',first_tunnel['media']['location']['city'])
                else:
                    loader.add_value('city',"None")
                
                if 'location' in first_tunnel['media'].keys():
                    loader.add_value('name_of_location',first_tunnel['media']['location']['name'])
                else:
                    loader.add_value('name_of_location',"None")
                
                if 'carousel_media_count' in first_tunnel['media'].keys():
                    loader.add_value('carousel_media_count',first_tunnel['media']['carousel_media_count'])
                else:
                    loader.add_value('carousel_media_count',0)
    
                yield loader.load_item()
                    
            # loader.add_value('pages', next_page)
            # loader.add_value('id', id_value)
        

        # yield loader.load_item()