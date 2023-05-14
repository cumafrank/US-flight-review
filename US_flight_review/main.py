import scrapy
from scrapy.crawler import CrawlerProcess
from US_flight_review.spiders.US_review_spider import UsSpider

company_lst = ['Alaska Airlines',
                            'Allegiant Air',
                            'American Airlines',
                            'Avelo Airlines',
                            'Breeze Airways',
                            'Delta Air Lines',
                            'Eastern Airlines',
                            'Frontier Airlines',
                            'Hawaiian Airlines',
                            'JetBlue Airways',
                            'Southwest Airlines',
                            'Spirit Airlines',
                            'Sun Country Airlines',
                            'United Airlines'
]

settings = {
    'BOT_NAME': 'US_flight_review',
    'SPIDER_MODULES': ['US_flight_review.spiders'],
    'NEWSPIDER_MODULE': 'US_flight_review.spiders',
    'COOKIES_ENABLED': False,
    'DOWNLOAD_DELAY': 3,
    'ROBOTSTXT_OBEY': True,
    'CONCURRENT_REQUESTS': 1,
    'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
    'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
    'FEED_EXPORT_ENCODING': 'utf-8',
    'ACTIVATION': True, # ON when it's activation job type, otherwise it's periodic
}

process = CrawlerProcess(settings=settings)
process.crawl(UsSpider, company_to_crawl=company_lst[0])
process.start() # the script will block here until the crawling is finished

# Optional procecure to manually activate crawling
#
#scrapy crawl US_review_spider -a company_index=0