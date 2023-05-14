import scrapy
from US_flight_review.items import UsFlightReviewItem
from US_flight_review.support import s3_snapshot
import re
from urllib.parse import urlencode


class UsSpider(scrapy.Spider):
    
    # Spider Meta Data
    name = "US_review_spider"
    allowed_domains = ["www.airlinequality.com"]
    custom_settings = {
        'ITEM_PIPELINES': {'US_flight_review.pipelines.UsFlightReviewPipeline': 300},
    }
    
    # Variable Storages
    def __init__(self, **kwargs):
        super(UsSpider, self).__init__(**kwargs)  # python3 way of taking kwargs
        self.company_to_crawl = kwargs.get('company_to_crawl',None)
        self.features = ['AirName',
                            'OverallScore',
                            'ReviewTitle',
                            'ReviewrCountry',
                            'ReviewDate',
                            'AircraftModel',
                            'TravelType',
                            'SeatType',
                            'Route',
                            'DateFlown',
                            'SeatComfortRating',
                            'ServiceRating',
                            'FoodRating',
                            'EntertainmentRating',
                            'GroundServiceRating',
                            'WifiRating',
                            'ValueRating',
                            'Recommended',
                            'Comments']
        latest_df = s3_snapshot(self)
        self.latest_date = max(latest_df['ReviewDate'])
        print(self.__dict__)
    
    def start_requests(self):
        target_url = 'https://www.airlinequality.com/airline-reviews/{}/page/{}?sortby=post_date%3ADesc&pagesize=3'
        for j in range(1,5-3):
            yield scrapy.Request(url=self.get_proxy_url(target_url.format('-'.join(self.company_to_crawl.split()).lower(), j)), callback=self.parse, meta={'company':self.company_to_crawl, 'page':j})

    def parse(self, response):
        reviews = response.xpath('//article[@itemprop="review"]')
        for review in reviews:
            
            #OverallScore
            try:
                OverallScore = review.xpath('./div/span[1]/text()').extract_first()
            except:
                OverallScore = ''


            #ReviewTitle
            try:
                ReviewTitle = review.xpath('././div/h2/text()').extract_first()
            except:
                ReviewTitle = ''


            #ReviewrCountry
            try:
                ReviewrCountry = response.xpath('//article[@itemprop="review"]/div/h3/text()').extract()[1]
            except:
                ReviewrCountry = ''


            #ReviewDate
            try:
                ReviewDate = review.xpath('././div/h3/time/text()').extract_first()
                ReviewDate = re.sub(r'\b\d+(st|nd|rd|th)\b', lambda m: m.group().rstrip('stndrdth'), ReviewDate)
            except:
                ReviewDate = ''


            #AircraftModel
            try:
                AircraftModel = review.xpath('.//td[@class="review-rating-header aircraft "]/../td[2]/text()').extract_first()
            except:
                AircraftModel = ''

            #TravelType
            try:
                TravelType = review.xpath('.//td[@class="review-rating-header type_of_traveller "]/../td[2]/text()').extract_first()
            except:
                TravelType = ''

            #SeatType
            try:
                SeatType = review.xpath('.//td[@class="review-rating-header cabin_flown "]/../td[2]/text()').extract_first()
            except:
                SeatType = ''

            #Route
            try:
                Route = review.xpath('.//td[@class="review-rating-header route "]/../td[2]/text()').extract_first()
            except:
                Route = ''

            #DateFlown
            try:
                DateFlown = review.xpath('.//td[@class="review-rating-header date_flown "]/../td[2]/text()').extract_first()
            except:
                DateFlown = ''

            #SeatComfortRating
            try:
                SeatComfortRatingPrep = review.xpath('.//td[@class="review-rating-header seat_comfort"]/../td[2]/span/@class').extract()
                SeatComfortRating = SeatComfortRatingPrep.count('star fill')
            except:
                SeatComfortRating = ''

            #ServiceRating
            try:
                ServiceRatingPrep = review.xpath('.//td[@class="review-rating-header cabin_staff_service"]/../td[2]/span/@class').extract()
                ServiceRating = ServiceRatingPrep.count('star fill')
            except:
                ServiceRating = ''

            #FoodRating
            try:
                FoodRatingPrep = review.xpath('.//td[@class="review-rating-header food_and_beverages"]/../td[2]/span/@class').extract()
                FoodRating = FoodRatingPrep.count('star fill')
            except:
                FoodRating = ''

            #EntertainmentRating
            try:
                EntertainmentRatingPrep = review.xpath('.//td[@class="review-rating-header inflight_entertainment"]/../td[2]/span/@class').extract()
                EntertainmentRating = EntertainmentRatingPrep.count('star fill')
            except:
                EntertainmentRating = ''

            #GroundServiceRating
            try:
                GroundServiceRatingPrep = review.xpath('.//td[@class="review-rating-header ground_service"]/../td[2]/span/@class').extract()
                GroundServiceRating = GroundServiceRatingPrep.count('star fill')
            except:
                GroundServiceRating = ''

            #WifiRating
            try:
                WifiRatingPrep = review.xpath('.//td[@class="review-rating-header wifi_and_connectivity"]/../td[2]/span/@class').extract()
                WifiRating = WifiRatingPrep.count('star fill')
            except:
                WifiRating = ''

            #ValueRating
            try:
                ValueRatingPrep = review.xpath('.//td[@class="review-rating-header value_for_money"]/../td[2]/span/@class').extract()
                ValueRating = ValueRatingPrep.count('star fill')
            except:
                ValueRating = ''

            #Recommended
            try:
                Recommended = review.xpath('.//td[@class="review-value rating-yes"]/text()').extract_first() 
            except:
                Recommended = 'no'

            #Comments
            try:
                Comments = response.xpath('//*[@id="anchor666219"]/div/div[1]/text()').extract_first()
            except:
                Comments = ''


            item = UsFlightReviewItem()
            item['AirName'] = response.meta.get('company')
            item['OverallScore'] = OverallScore
            item['ReviewTitle'] = ReviewTitle
            item['ReviewrCountry'] = ReviewrCountry
            item['ReviewDate'] = ReviewDate
            item['AircraftModel'] = AircraftModel
            item['TravelType'] = TravelType
            item['SeatType'] = SeatType
            item['Route'] = Route
            item['DateFlown'] = DateFlown
            item['SeatComfortRating'] = SeatComfortRating
            item['ServiceRating'] = ServiceRating
            item['FoodRating'] = FoodRating
            item['EntertainmentRating'] = EntertainmentRating
            item['GroundServiceRating'] = GroundServiceRating
            item['WifiRating'] = WifiRating
            item['ValueRating'] = ValueRating
            item['Recommended'] = Recommended
            item['Comments'] = Comments

            yield item
        pass
    
    # Helper function: proxy rotation
    def get_proxy_url(self, url):
        API_KEY = 'a8f16a6c-2482-4bdb-812a-eed1964a6796'
        
        payload = {'api_key': API_KEY, 'url': url}
        proxy_url = 'https://proxy.scrapeops.io/v1/?' + urlencode(payload)
        return proxy_url