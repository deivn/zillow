# -*- coding: utf-8 -*-
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy_redis.spiders import RedisSpider
import re
import scrapy
import time
import redis
import json
from decimal import Decimal
from zillow.items import ZillowItem


class ZillowSpider(RedisSpider):
    name = 'zillow'
    # 增加redis_keys
    redis_key = 'zillowspider:start_urls'
    allowed_domains = ['zillow.com']
    # detail_queue = redis.Redis(host='47.106.140.94', port='6486', db=2, decode_responses=True)
    # https://www.zillow.com/homes/for_rent/NV/42_rid/50.694718,-98.063965,22.370396,-145.305176_rect/4_zm/
    # page_helper_lx = LinkExtractor(allow=(r'^https://www.zillow.com/homes/for_rent/\w+/42_rid/[-]?\d+\.\d+,[-]?\d+\.\d+,[-]?\d+\.\d+,[-]?\d+\.\d+_rect/4_zm/\d+_p/$'))
    # https://www.zillow.com/homes/for_rent/NV/5XhwpD_bldg/42_rid/50.694718,-98.063965,22.370396,-145.305176_rect/4_zm/
    # https://www.zillow.com/homedetails/2509-NW-39th-Ter-Oklahoma-City-OK-73112/21856838_zpid/
    # house_detail = LinkExtractor(allow=(r'^https://www.zillow.com/homes/for_rent/\w+/\d+_zpid/\d+_rid.+'))
    # house_detail = LinkExtractor(allow=(r'^https://www.zillow.com/homedetails.+/\d+_zpid/$'))

    # rules = (
    #     # Rule(page_helper_lx, follow=True),
    #     Rule(house_detail, callback='parse_item', follow=False),
    # )

    # def __init__(self, *args, **kwargs):
    #     domain = kwargs.pop('domain', '')
    #     self.allowed_domains = filter(None, domain.split(','))
    #     super(ZillowSpider, self).__init__(*args, **kwargs),
    detail_queue = redis.Redis(host='47.106.140.94', port='6486', db=2, decode_responses=True)

    def parse(self, response):
        time.sleep(3)
        result = self.detail_queue.spop("content")
        if result:
            detail = json.loads(result)
            if "url" in detail.keys():
                detail["zipcode"] = detail["detail"]["zipcode"]
                detail["latitude"] = detail["detail"]["latitude"]
                detail["longitude"] = detail["detail"]["longitude"]
                detail["city"] = detail["detail"]["city"]
                detail["state"] = detail["detail"]["state"]
            url = detail["url"] if "url" in detail.keys() else detail["detail_url"]
            detail['zpid'] = detail["zpid"] if "zpid" in detail.keys() else self.get_zpid(url)
            print("正在爬取的url: %s" % url)
            yield scrapy.Request(url, callback=self.parse_item, meta={"detail": detail})

    def parse_item(self, response):
        item = ZillowItem()
        detail = response.meta['detail']
        item["url"] = response.url
        item["price"] = detail["price"] if "price" in detail.keys() else self.get_price(response)
        item["bedroom"] = detail["bedrooms"] if "bedrooms" in detail.keys() else self.get_bedroom(response)
        item["bathroom"] = detail["bathrooms"] if "bathrooms" in detail.keys() else self.get_bathroom(response)
        item["street"] = detail["streetAddress"] if "streetAddress" in detail.keys() else self.get_street(response)
        item["deal_type"] = self.get_dealtype(response)
        item["img_url"] = self.get_imgurl(response)
        item["living_sqft"] = detail["livingArea"] if "livingArea" in detail.keys() else self.get_livingsqft(response)
        item["comments"] = self.get_desc(response).replace("\n", "")
        item["agent"] = self.get_agent(response)
        item["house_type"] = self.get_info_by_keyword(response, 'Type:')
        item["heating"] = self.get_heating(response)
        item["cooling"] = self.get_cooling(response)
        item["price_sqft"] = self.get_pricesqft(response)
        # Year built
        year_build = detail["yearBuilt"] if "yearBuilt" in detail.keys() else self.get_info_by_keyword(response, 'Year built:')
        if not year_build:
            year_build = self.get_yearbuild(response, 'Year built')
        item["year_build"] = year_build
        item["parking"] = self.get_parking(response)
        item["lot_sqft"] = detail["lotSize"] if "lotSize" in detail.keys() else self.get_lotsqft(response)
        item["hoa_fee"] = self.get_hoafee(response)
        item["mls"] = self.get_mls(response)
        item["apn"] = self.get_apn(response)
        item["garage"] = self.get_garage(response, 'Parking:')
        item["deposit"] = self.get_info_by_keyword(response, 'Deposit & fees:')
        item["contact_phone"] = self.get_contactphone(response)
        item["contact_name"] = self.get_contactname(response)
        item["zipcode"] = detail["zipcode"]
        item["latitude"] = detail["latitude"]
        item["longitude"] = detail["longitude"]
        item["city"] = detail["city"]
        item["state"] = detail["state"]
        yield item





        # item['img_url'] = self.get_imgurl(response)
        # item['comments'] = self.get_desc(response)
        # item['house_type'] = self.get_info_by_keyword(response, 'Type:')
        # item['parking'] = self.get_parking(response)
        # item['hoa_fee'] = self.get_hoafee(response)
        # item['contact_name'] = self.get_contactname(response)
        # item['contact_phone'] = self.get_contactphone(response)
        # item['mls'] = self.get_mls(response)
        # item['apn'] = self.get_apn(response)
        # item['garage'] = self.get_garage(response, 'Parking:')
        # item['deposit'] = self.get_info_by_keyword(response, 'Deposit & fees:')


    def get_lotsqft(self, source):
        lotsqft = source.xpath('//header[@class="ds-bed-bath-living-area-header"]/h3/span[3]/span[1]/text()').extract()
        lot_unit = source.xpath('//header[@class="ds-bed-bath-living-area-header"]/h3/span[3]/span[2]/text()').extract()
        lot = "0"
        if lotsqft:
            lot = lotsqft[0].replace(" ", "")
        if lot_unit:
            unit = lot_unit[0].replace(" ", "")
            if unit == 'acres':
               lot = str(Decimal(lot) * Decimal(43560))
        return lot

    def get_yearbuild(self, source, keyword):
        yearbuild = source.xpath('//li[@class="ds-sub-section-container"]//td[contains(text(), "'+keyword+'")]/following-sibling::td/span/text()').extract()
        return yearbuild[0].replace(" ", "") if yearbuild else ""

    def get_pricesqft(self, source):
        pricesqft = source.xpath('//ul[@class="ds-home-fact-list"]/li[7]/span[2]/text()').extract()# 每平方英尺价格
        return pricesqft[0] if pricesqft else ""

    def get_heating(self, source):
        heating = source.xpath('//ul[@class="ds-home-fact-list"]/li[3]/span[2]/text()').extract()# 暖气
        return heating[0].replace(" ", "") if heating else ""

    def get_cooling(self, source):
        cooling = source.xpath('//ul[@class="ds-home-fact-list"]/li[4]/span[2]/text()').extract()# 冷气
        return cooling[0] if cooling else ""

    def get_agent(self, source):
        agent = source.xpath('//div[@class="ds-overview-agent-card"]/div[1]/text()').extract()
        return agent[0] if agent else ""

    def get_livingsqft(self, source):
        livingsqft = source.xpath('//span[@class="ds-bed-bath-living-area"]/span/text()').extract()
        livingsize = "0"
        if livingsqft:
            livingsize = livingsqft[0].replace(" ", "")
            unit = livingsqft[1].replace(" ", "")
            if unit == 'acres':
                livingsize = str(Decimal(livingsize) * Decimal(43560))
        return livingsize

    def get_dealtype(self, source):
        deal_type = source.xpath('//div[@class="ds-chip-removable-content"]//span[@class="ds-status-details"]/text()').extract()
        return deal_type[0] if deal_type else ""

    def get_street(self, source):
        street_result = source.xpath('//h1[@class="ds-address-container"][1]/span/text()').extract()
        _street = ''
        if len(street_result) > 3:
            a = ''.join(street_result)
            b = a.replace("\xa0", "")
            c = re.match(r'(\d+)?.+', b)
            if c:
                d = c.groups()[0]
                if d.isdigit():
                    _street = d + b.split(d)[1]
        return _street

    def get_bathroom(self, response):
        bathroom = response.xpath('//header[@class="ds-bed-bath-living-area-header"]/h3/span[2]/span[1]/text()').extract()
        bath = ""
        if bathroom:
            bath = bathroom[len(bathroom) - 1].replace(" ", "")
        return bath

    def get_bedroom(self, response):
        bedroom = response.xpath('//h3[@class="ds-bed-bath-living-area-container"]/span[1]/span[1]/text()').extract()
        bd = ""
        if bedroom:
            bd = bedroom[len(bedroom) - 1].replace(" ", "")
        return bd

    def get_price(self, source):
        r = ""
        price_result = source.xpath('//h3[@class="ds-price"]/span')
        if price_result:
            if len(price_result) == 2:
                price = price_result.xpath('./span[1]/text()').extract()[0].replace(" ", "")
                unit = price_result.xpath('./span[2]/text()').extract()[0].replace(" ", "")
                r = price + unit
        else:
            price_result = source.xpath('//h3[@class="ds-price"]/span/span[contains(text(), "$")]/text()').extract()
            if price_result:
                r = price_result[0].replace(" ", "").replace(" ", "")
        return r

    def get_contactname(self, response):
        agent_name = response.xpath('//ul[@class="ds-listing-agent-info"]/li[1]/span/text()').extract()
        return agent_name[0] if agent_name else ""

    def get_contactphone(self, response):
        contact_phone = response.xpath('//ul[@class="ds-listing-agent-info"]/li[3]/text()').extract()
        if not contact_phone:
            contact_phone = response.xpath('//ul[@class="ds-listing-agent-info"]/li[2]/text()').extract()
            if not contact_phone:
                contact_phone = response.xpath('//div[@class="cf-cnt-rpt-sig-info"]/span[@data-test-id="cf-contact-phone-number"]/text()').extract()
        return contact_phone[0] if contact_phone else ""


    def get_imgurl(self, response):
        img_url = response.xpath('//ul[@class="media-stream"]/li/picture//img/@src').extract()
        return ','.join(img_url) if img_url else ""

    def get_desc(self, response):
        desc = response.xpath('//div[@class="ds-overview-section"]/div[1]/div[1]/text()').extract()
        return desc[0] if desc else ""

    def get_garage(self, response, keyword):
        item = ""
        tmp = response.xpath('//ul[@class="ds-home-fact-list"]/li[@class="ds-home-fact-list-item"]//span[text()="'+keyword+'"]/following-sibling::span/text()').extract()
        if tmp:
            item = tmp[0]
        return item

    def get_apn(self, response):
        apn_result = response.xpath('//td[text() = "Parcel number"]/following-sibling::td/span/text()').extract()
        return apn_result[0].replace(" ", "") if apn_result else ""


    def get_mls(self, response):
        mls_result = response.xpath('//td[text() = "MLS ID"]/following-sibling::td/span/text()').extract()
        return mls_result[0].replace(" ", "") if mls_result else ""


    def get_hoafee(self, response):
        hoafee = response.xpath('//td[text() = "Has HOA fee"]/following-sibling::td/span/text()').extract()
        return hoafee[0] if hoafee else ""


    def get_info_by_keyword(self, response, keyword):
        result = response.xpath('//ul[@class="ds-home-fact-list"]/li[@class="ds-home-fact-list-item"]//span[text()="'+keyword+'"]/following-sibling::span/text()').extract()
        return result[0] if result else ''

    def get_parking(self, response):
        parking = response.xpath('//ul[@class="ds-home-fact-list"]/li[5]/span[2]/text()').extract()
        if not parking:
            parking = response.xpath('//li[@class="ds-home-fact-list-item"]//span[contains(text(), "Parking:")]/following-sibling::span/text()').extract()
        return parking[0] if parking else ""

    def get_zpid(self, url):
        _zpid = re.match(r'^https://www.zillow.com/homedetails/(\d+)_zpid', url)
        return _zpid.groups()[0]
