import scrapy
from urllib.parse import urlencode

class AirlinesSpider(scrapy.Spider):
    name = 'US_com_spider'
    start_urls = ['https://en.wikipedia.org/wiki/List_of_airlines_of_the_United_States']
    custom_settings = {
        'ITEM_PIPELINES': {'US_flight_review.pipelines.UsCompanyPipeline': 300},
    }
    
    # Function activated when start scraping
    def start_requests(self):
        start_url = 'https://en.wikipedia.org/wiki/List_of_airlines_of_the_United_States'
        yield scrapy.Request(url=self.get_proxy_url(start_url), callback=self.parse)

    # Parse function
    def parse(self, response):
        print(response)
        for row in response.xpath("//table[@class='wikitable sortable jquery-tablesorter'][1]//tbody//tr"):
            yield {
                'name': row.xpath(".//td[1]//text()").get(),
                'iata_code': row.xpath(".//td[2]//text()").get(),
                'icao_code': row.xpath(".//td[3]//text()").get(),
                'callsign': row.xpath(".//td[4]//text()").get(),
                'founded': row.xpath(".//td[5]//text()").get(),
                'commenced_operations': row.xpath(".//td[6]//text()").get(),
                'ceased_operations': row.xpath(".//td[7]//text()").get(),
                'notes': row.xpath(".//td[8]//text()").get(),
            }
    
    # Helper function: proxy rotation
    def get_proxy_url(self, url):
        API_KEY = 'a8f16a6c-2482-4bdb-812a-eed1964a6796'
        
        payload = {'api_key': API_KEY, 'url': url}
        proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
        return proxy_url