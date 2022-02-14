import scrapy
from scrapy.item import Field
from itemloaders.processors import TakeFirst, MapCompose, Join
from scrapy.loader import ItemLoader
from scrapy.crawler import CrawlerProcess
from scrapy.http import JsonRequest
import pandas as pd

headers = { 'authority': 'www.tripadvisor.co.uk',
    'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'sec-ch-ua-mobile': '?0',
    'x-requested-by': 'TNI1625!AG1YRRpHOjQMgbfsrg1FWY4Ai8UH+StE3D7tD1/oCg3qzWRAYM2ff14YfUM2JUbFAl0x6vTP5McIcIHK3vGsWp/OUNzOT5pIGiZKb0BGLlQkrHttvrrkMiEX1B08Oy4WjTHFseLIh9VcHJi4Gh0/+LjAQFKarv7VPh3A6Lba2SV/',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 Safari/537.36',
    'sec-ch-ua-platform': '"macOS"',
    'accept': '*/*',
    'origin': 'https://www.tripadvisor.co.uk',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    #'referer': 'https://www.tripadvisor.co.uk/Cruises-g4-Europe-Cruises',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    #'cookie': 'TADCID=wxS4TbuLpUspXUnWABQCFdpBzzOuRA-9xvCxaMyI12uHBUXU8sLyLHDaoIuwxzQKyBrTFlgsk84ZsL_itZEhwu8hHz-VItOKi2w; TAUnique=%1%enc%3A8kwUAflygK31tlwOhg%2Fo76dh9wxu05Ut4MznwnYlPlg2jHwltRJPGQ%3D%3D; TASSK=enc%3AAPiLXN0t%2B8Q%2Fy9%2FxR%2BD5555CPTdegwxcaa1ok4l9U33f3IyY6Qg8GN25OIJ4ccUZntma4TTL9a%2Bl%2BoIz%2FTAucOZ2TqYV6tkQbMAYMyq1l5ArmPX7CjgQq2QO%2B9HE%2BLVGaw%3D%3D; ServerPool=X; PMC=V2*MS.18*MD.20220214*LD.20220214; TART=%1%enc%3A9bZcDoYP6O8GE%2BreSp1djAImFcYdHhqBzveZGrQkjMRhW3dIKV4FZ%2FdZWju6gRL2CeyaC1LeImE%3D; TATravelInfo=V2*A.2*MG.-1*HP.2*FL.3*RS.1; TASID=A32BBADDF2344464B10CA8620CFBF2AF; TAReturnTo=%1%%2FCruises-g4-Europe-Cruises; ak_bmsc=D33D58B4C6C12D1D96C6D50E4A8267F6~000000000000000000000000000000~YAAQ3Jl6XLHYnul+AQAAegxz9w6zDYaRpZULIXbmwExafFXbLk88He8U5RsFRJJHYrPKRs60IK77pXrkBd1Bl7bvDGDEhZqKkbtOP/6nqDF1R4eUq2ZuIfReBoo+S9nxAuR2rla11JjDVD65qUN1aH0uichlgPClLxslcNh3JclKJzPv3kg7aDgrvT2CaDQ5f5zz2UPkb+EOkAEyOPwhg8exOgHhsbD2BhGqL7PAOPfZPVuocBXutZBOcDBrsy1rZlHC79MQQdX5szmK9zwQnZUVDvmln+DUVXXyN835bImRRSbTNz12EDee2RgtZwmuQNv+eSXnS3gJHBkTErdp7jEdbbCytqTdI2Ix8OR8QzmJnUAXL0dOvpqmUmkGFnWxUz68QxHkf7hC91Pqt3CJ2A==; OptanonAlertBoxClosed=2022-02-14T08:57:03.130Z; eupubconsent-v2=CPUZP2aPUZP23AcABBENCCCsAP_AAH_AACiQIltf_X__b3_j-_5_f_t0eY1P9_7_v-0zjhfdt-8N3f_X_L8X42M7vF36pq4KuR4Eu3LBIQdlHOHcTUmw6okVrzPsbk2cr7NKJ7PEmnMbO2dYGH9_n93TuZKY7______z_v-v_v____f_7-3_3__5_3---_e_V_99zLv9____39nP___9v-_9_____4IhgEmGpeQBdmWODJtGlUKIEYVhIdAKACigGFoisIHVwU7K4CfUELABCagJwIgQYgowYBAAIJAEhEQEgB4IBEARAIAAQAqQEIACNgEFgBYGAQACgGhYgRQBCBIQZHBUcpgQFSLRQT2ViCUHexphCGWeBFAo_oqEBGs0QLAyEhYOY4AkBLxZIHmKF8gAAAAA.f_gAD_gAAAAA; OTAdditionalConsentString=1~39.43.46.55.61.66.70.83.89.93.108.117.122.124.131.135.136.143.144.147.149.159.162.167.171.192.196.202.211.218.228.230.239.241.259.266.272.286.291.311.317.322.323.326.327.338.367.371.385.389.394.397.407.413.415.424.430.436.440.445.449.453.482.486.491.494.495.501.503.505.522.523.540.550.559.560.568.574.576.584.587.591.733.737.745.780.787.802.803.817.820.821.829.839.864.867.874.899.904.922.931.938.979.981.985.1003.1024.1027.1031.1033.1034.1040.1046.1051.1053.1067.1085.1092.1095.1097.1099.1107.1127.1135.1143.1149.1152.1162.1166.1186.1188.1201.1205.1211.1215.1226.1227.1230.1252.1268.1270.1276.1284.1286.1290.1301.1307.1312.1345.1356.1364.1365.1375.1403.1415.1416.1419.1440.1442.1449.1455.1456.1465.1495.1512.1516.1525.1540.1548.1555.1558.1564.1570.1577.1579.1583.1584.1591.1603.1616.1638.1651.1653.1665.1667.1677.1678.1682.1697.1699.1703.1712.1716.1721.1722.1725.1732.1745.1750.1765.1769.1782.1786.1800.1808.1810.1825.1827.1832.1837.1838.1840.1842.1843.1845.1859.1866.1870.1878.1880.1889.1899.1917.1929.1942.1944.1962.1963.1964.1967.1968.1969.1978.2003.2007.2008.2027.2035.2039.2044.2046.2047.2052.2056.2064.2068.2070.2072.2074.2088.2090.2103.2107.2109.2115.2124.2130.2133.2137.2140.2145.2147.2150.2156.2166.2177.2183.2186.2202.2205.2216.2219.2220.2222.2225.2234.2253.2264.2279.2282.2292.2299.2305.2309.2312.2316.2322.2325.2328.2331.2334.2335.2336.2337.2343.2354.2357.2358.2359.2366.2370.2376.2377.2387.2392.2394.2400.2403.2405.2407.2411.2414.2416.2418.2425.2427.2440.2447.2459.2461.2462.2465.2468.2472.2477.2481.2484.2486.2488.2492.2493.2496.2497.2498.2499.2501.2510.2511.2517.2526.2527.2532.2534.2535.2542.2544.2552.2563.2564.2567.2568.2569.2571.2572.2575.2577.2583.2584.2589.2595.2596.2601.2604.2605.2608.2609.2610.2612.2614.2621.2628.2629.2633.2634.2636.2642.2643.2645.2646.2647.2650.2651.2652.2656.2657.2658.2660.2661.2669.2670.2677.2681.2684.2686.2687.2690.2695.2698.2707.2713.2714.2729.2739.2767.2768.2770.2772.2784.2787.2791.2792.2798.2801.2805.2812.2813.2816.2817.2818.2821.2822.2827.2830.2831.2834.2836.2838.2839.2840.2844.2846.2847.2849.2850.2851.2852.2854.2856.2860.2862.2863.2865.2867.2869.2873.2874.2875.2876.2878.2879.2880.2881.2882.2883.2884.2886.2887.2888.2889.2891.2893.2894.2895.2897.2898.2900.2901.2908.2909.2911.2912.2913.2914.2916.2917.2918.2919.2920.2922.2923.2924.2927.2929.2930.2931.2939.2940.2941.2942.2947.2949.2950.2956.2961.2962.2963.2964.2965.2966.2968.2970.2973.2974.2975.2979.2980.2981.2983.2985.2986.2987.2991.2993.2994.2995.2997.2999.3000.3002.3003.3005.3008.3009.3010.3012.3016.3017.3018.3019.3024.3025.3028.3034.3037.3038.3043.3044.3045.3048.3052.3053.3055.3058.3059.3063.3065.3066.3068.3070.3072.3073.3074.3075.3076.3077.3078.3089.3090.3093.3094.3095.3097.3099.3100.3104.3106.3109.3111.3112.3116.3117.3118.3119.3120.3124.3126.3127.3128.3130.3135.3136.3145.3149.3150.3151.3154.3155.3162.3163.3167.3172.3173.3180.3182.3183.3184.3185.3187.3188.3189.3190.3194.3196.3197.3209.3210.3211.3214.3215.3217.3219.3222.3223.3225.3226.3227.3228.3230.3231.3232.3234.3235.3236.3237.3238.3240.3241.3244.3245.3250.3251.3253.3257.3260.3268.3270.3272.3281.3288.3290.3292.3293.3295.3296; TATrkConsent=eyJvdXQiOiIiLCJpbiI6IkFMTCJ9; PAC=AJukZreSlVt2otjGKRNkBz00tWSjLZs1tpXwS8IQ0s9vLyuOrKUvS1c6om5r-WD0fR_Iq3GAZVuS7Hnkp36pQwhrEE0TfQD_2HKg4iY1nBIuQhuDqCWdbnFs1YNDeC2DHqRS5g91y4fgvYu2t67DsbY-k350iSZC1V5Q8MOom6ii; roybatty=TNI1625!APC1CnSJ7d3OhZC8OZmN5URwrla0tLHPbhlztWxjhZhT6aUqZSiQblRTSzow7ftctB099qedPBwThnzphE8mD%2BhqV6BvNYPIhvySRzEFTVzRp06wXxRc8ZRTjzdR%2B6TMtg6r4C0frqplHn1ukZ4jm5nriuS8VgJVY1P1ep6OFaNM%2C1; __vt=wzYY-5Il_FhNE1AIABQCIf6-ytF7QiW7ovfhqc-AvRtk3_lgKJDj5Zq9Ugk-YcW1aWXqbclfQV6lVC3XwLDW4R4P6wRyFZBracNMyGIQ5t0P83yLijLokcFANA9-zVQ698yGW3svmERyK7AnfVnyS4CdjUA; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Feb+14+2022+09%3A54%3A26+GMT%2B0000+(Greenwich+Mean+Time)&version=6.30.0&isIABGlobal=false&hosts=&consentId=bd1952f6-01ff-41b6-861f-a54916a04f3b&interactionCount=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CSTACK42%3A1&geolocation=GB%3BENG&AwaitingReconsent=false; SRT=%1%enc%3A9bZcDoYP6O8GE%2BreSp1djAImFcYdHhqBzveZGrQkjMRhW3dIKV4FZ%2FdZWju6gRL2CeyaC1LeImE%3D; TASession=V2ID.A32BBADDF2344464B10CA8620CFBF2AF*SQ.11*LS.PageMoniker*GR.86*TCPAR.46*TBR.15*EXEX.62*ABTR.9*PHTB.8*FS.28*CPU.39*HS.recommended*ES.popularity*DS.5*SAS.popularity*FPS.oldFirst*FA.1*DF.0*TRA.true*LD.4*EAU._; TAUD=LA-1644829020144-1*RDD-1-2022_02_14*LG-3446857-2.1.F.*LD-3446858-.....; bm_sv=2A36E698463670EE6568739F8CDB1175~3+hcdvQLIRwah/ob3yiC6FDLIUMklns+OmkkhCI+VXdPQ9Cu0Tgp1gj42eltojUxM4qnZc+AQhSLEPtZgkZVPf7jtaIT9dgLdeJFsXrByFiSKAtnDYW8m7bd+9XZCOjX0Vs6okcP/XE3YBv7UlJP6aVMfxFgMK5VPFHA9GoE1IA=',
}



class CruisesItem(scrapy.Item):
    ID = Field(output_processor = TakeFirst())
    title = Field(output_processor = TakeFirst())
    crew = Field(output_processor = TakeFirst())
    capacity = Field(output_processor = TakeFirst())
    ship_name = Field(output_processor = TakeFirst())
    overallRatings = Field(output_processor = TakeFirst())
    numberReview = Field(output_processor = TakeFirst())
    amenities = Field()
    departureDate = Field()
    portArrival = Field(output_processor = TakeFirst())
    portDeparture = Field(output_processor = TakeFirst())
    itinerary = Field()
    reviewUrl = Field(output_processor = TakeFirst())
    destination = Field(output_processor = TakeFirst())



class CruisesSpider(scrapy.Spider):
    name = 'cruises_no_sel'
    start_urls = ['https://www.tripadvisor.co.uk/data/graphql/ids']
    # custom_settings = {
    #     'DOWNLOAD_DELAY':1
    # }

    def start_requests(self):
        for urls in self.start_urls:
            for i in range(1, 600):
                yield JsonRequest(
                    url = urls, method = 'POST',callback = self.parse,
                    headers = headers,
                    data = [
                            {
                                'query': '013d760a68c9a4f77e9a9a903e241eb8',
                                'variables': {
                                    'page': i,
                                    'limit': 20,
                                    'minPrice': None,
                                    'maxPrice': None,
                                    'order': 'popularity',
                                    'itineraryId': None,
                                    'vendorId': None,
                                    'cruiseLineId': None,
                                    'shipId': None,
                                    'cabinType': None,
                                    'departureDate': None,
                                    'length': None,
                                    'destinationId': [],
                                    'departurePortId': None,
                                    'portId': None,
                                    'cruiseStyleId': None,
                                    'dealId': None,
                                    'viewport': 'small',
                                    'locale': 'en_UK',
                                    'currency': 'GBP',
                                },
                            },
                        ],

            )
    def parse(self, response):
        container = response.json()
        for results in container:
            
            for data_results in results['data']['cruiseList']['results']:
                loader = ItemLoader(CruisesItem())
                loader.add_value('ID',data_results['lowestPricedSailing']['id'])
                loader.add_value('title',data_results['title'])
                loader.add_value('crew',data_results['ship']['crew'])
                loader.add_value('capacity',data_results['ship']['passengerCapacity'])
                loader.add_value('ship_name',data_results['ship']['name'])
                try:
                    loader.add_value('overallRatings',data_results['ship']['reviewSummaryInfo']['overallRatings'])
                except:
                    continue
                try:
                    loader.add_value('numberReview',data_results['ship']['reviewSummaryInfo']['numberReviews'])
                except:
                    continue

                loader.add_value('amenities',data_results['ship']['amenities']['results'])
                loader.add_value('destination',data_results['destination']['seoName'])
                loader.add_value('departureDate',data_results['sailings']['results'])
                loader.add_value('portArrival',data_results['portArrival']['name'])
                loader.add_value('portDeparture',data_results['portDeparture']['name'])
                loader.add_value('itinerary',data_results['itinerary']['results'])
                try:
                    loader.add_value('reviewUrl','https://www.tripadvisor.co.uk' + data_results['ship']['reviewUrl'])
                except:
                    continue
                yield loader.load_item()
    # def parse_data(self, response):


process = CrawlerProcess(
    settings = {
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 Safari/537.36',
        'FEEDS':{
        'cruises.jl':{
            'format':'jsonlines'
        }
    }
    }
)
process.crawl(CruisesSpider)
process.start()
