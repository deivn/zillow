# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ZillowItem(scrapy.Item):
    price = scrapy.Field()
    bedroom = scrapy.Field()
    bathroom = scrapy.Field()
    street = scrapy.Field()
    deal_type = scrapy.Field()
    img_url = scrapy.Field()
    living_sqft = scrapy.Field()
    comments = scrapy.Field()
    agent = scrapy.Field()
    house_type = scrapy.Field()
    heating = scrapy.Field()
    cooling = scrapy.Field()
    price_sqft = scrapy.Field()
    year_build = scrapy.Field()
    parking = scrapy.Field()
    lot_sqft = scrapy.Field()
    hoa_fee = scrapy.Field()
    contact_phone = scrapy.Field()
    contact_name = scrapy.Field()
    url = scrapy.Field()
    mls = scrapy.Field()
    apn = scrapy.Field()
    garage = scrapy.Field()
    deposit = scrapy.Field()
    zip = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()
    city = scrapy.Field()
    state = scrapy.Field()
    zipcode = scrapy.Field()
