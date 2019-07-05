#! /usr/bin/env python  
# -*- coding:utf-8 -*-
from selenium import webdriver
from lxml import etree
import json
import requests
import codecs
import redis
import re
from decimal import Decimal
from data import Data
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import threading
import time
from queue import Queue
que = Queue(5)


class Producer(threading.Thread):
    """
    生产html
    """
    def __init__(self, dataopt):
        threading.Thread.__init__(self)
        self.dataopt = dataopt

    def run(self):
        global que
        total = self.dataopt.detail_queue.scard("content")
        while total:
            if que.empty():
                result = self.dataopt.detail_queue.spop("content")
                if result:
                    _json = json.loads(result)
                    self.dataopt.get_ip()
                    browser = self.dataopt.star_chr()
                    try:
                        url = _json['detail_url']
                        html = self.dataopt.origin_page(url, browser)
                        print("producer %s product %s, the rest of requests %d in queue." % (self.name, url, self.dataopt.detail_queue.scard("content")))
                        que.put((url, html, _json))
                    except Exception as e:
                        info = e.args[0]
                        if info == 'robots':
                            self.dataopt.get_ip()
                        self.dataopt.detail_queue.sadd("content", result)
                        time.sleep(5)
                    finally:
                        browser.quit()
        else:
            raise Exception("request empty")


class Consumer(threading.Thread):
    """"
    消费html
    """
    def __init__(self, dataopt):
        threading.Thread.__init__(self)
        self.dataopt = dataopt

    def run(self):
        global que
        while True:
            if not que.empty():
                url, html, detail = que.get()
                page_deal = PageDeal(detail, html, self.dataopt.re_queue)
                print("consumer %s consume %s" % (self.name, url))
                page_deal.data_page()
                time.sleep(10)
                # 发出完成的信号，不发的话，join会永远阻塞，程序不会停止
                que.task_done()


class PageDeal(object):
    """解析类"""
    def __init__(self, detail, html, re_queue):
        self.detail = detail
        self.html = html
        self.re_queue = re_queue

    def get_info_by_keyword(self, source, keyword):
        result = source.xpath('//ul[@class="ds-home-fact-list"]/li[@class="ds-home-fact-list-item"]//span[text()="'+keyword+'"]/following-sibling::span/text()')
        return result[0] if result else ''

    def get_yearbuild(self, source):
        yearbuild = source.xpath('//li[@class="ds-sub-section-container"]//td[contains(text(), "Year built")]/following-sibling::td/span/text()')
        return yearbuild[0].replace(" ", "") if yearbuild else ""

    def get_price(self, source):
        r = ""
        price_result = source.xpath('//h3[@class="ds-price"]/span')
        if price_result:
            if len(price_result) == 2:
                price = price_result[0].xpath('./span[1]/text()')[0].replace(" ", "")
                unit = price_result[0].xpath('./span[2]/text()')[0].replace(" ", "")
                r = price + unit
        else:
            price_result = source.xpath('//h3[@class="ds-price"]/span/span[contains(text(), "$")]/text()')
            if price_result:
                r = price_result[0].replace(" ", "").replace(" ", "")
        return r

    def get_bedroom(self, response):
        bedroom = response.xpath('//h3[@class="ds-bed-bath-living-area-container"]/span[1]/span[1]/text()')
        bd = ""
        if bedroom:
            bd = bedroom[len(bedroom) - 1].replace(" ", "")
        return bd

    def get_bathroom(self, response):
        bathroom = response.xpath('//header[@class="ds-bed-bath-living-area-header"]/h3/span[2]/span[1]/text()')
        bath = ""
        if bathroom:
            bath = bathroom[len(bathroom) - 1].replace(" ", "")
        return bath

    def get_street(self, source):
        street_result = source.xpath('//h1[@class="ds-address-container"][1]/span/text()')
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

    def get_dealtype(self, source):
        deal_type = source.xpath('//div[@class="ds-chip-removable-content"]//span[@class="ds-status-details"]/text()')
        if deal_type:
            deal_type = source.xpath('//span[@class="ds-status-details"]/text()')
        return deal_type[0] if deal_type else ""

    def get_imgurl(self, source):
        img_url = source.xpath('//ul[@class="media-stream"]/li/picture//img/@src')
        return ','.join(img_url) if img_url else ""

    def get_livingsqft(self, source):
        livingsqft = source.xpath('//span[@class="ds-bed-bath-living-area"]/span/text()')
        livingsize = "0"
        if livingsqft:
            livingsize = livingsqft[0].replace(" ", "")
            unit = livingsqft[1].replace(" ", "")
            if unit == 'acres':
                livingsize = str(Decimal(livingsize) * Decimal(43560))
        return livingsize

    def get_desc(self, source):
        desc = source.xpath('//div[@class="ds-overview-section"]/div[1]/div[1]/text()')
        return desc[0] if desc else ""

    def get_agent(self, source):
        agent = source.xpath('//div[@class="ds-overview-agent-card"]/div[1]/text()')
        return agent[0] if agent else ""


    def get_heating(self, source):
        heating = source.xpath('//ul[@class="ds-home-fact-list"]/li[3]/span[2]/text()')# 暖气
        return heating[0].replace(" ", "") if heating else ""

    def get_cooling(self, source):
        cooling = source.xpath('//ul[@class="ds-home-fact-list"]/li[4]/span[2]/text()')# 冷气
        return cooling[0] if cooling else ""

    def get_pricesqft(self, source):
        pricesqft = source.xpath('//ul[@class="ds-home-fact-list"]/li[7]/span[2]/text()')# 每平方英尺价格
        return pricesqft[0] if pricesqft else ""

    def get_parking(self, source):
        parking = source.xpath('//ul[@class="ds-home-fact-list"]/li[5]/span[2]/text()')
        if not parking:
            parking = source.xpath('//li[@class="ds-home-fact-list-item"]//span[contains(text(), "Parking:")]/following-sibling::span/text()')
        return parking[0] if parking else ""

    def get_lotsqft(self, source):
        lotsqft = source.xpath('//header[@class="ds-bed-bath-living-area-header"]/h3/span[3]/span[1]/text()')
        lot_unit = source.xpath('//header[@class="ds-bed-bath-living-area-header"]/h3/span[3]/span[2]/text()')
        lot = "0"
        if lotsqft:
            lot = lotsqft[0].replace(" ", "")
        if lot_unit:
            unit = lot_unit[0].replace(" ", "")
            if unit == 'acres':
               lot = str(Decimal(lot) * Decimal(43560))
        return lot

    def get_hoafee(self, source):
        hoafee = source.xpath('//td[text() = "Has HOA fee"]/following-sibling::td/span/text()')
        return hoafee[0] if hoafee else ""

    def get_mls(self, source):
        mls_result = source.xpath('//td[text() = "MLS ID"]/following-sibling::td/span/text()')
        return mls_result[0].replace(" ", "") if mls_result else ""

    def get_apn(self, source):
        apn_result = source.xpath('//td[text() = "Parcel number"]/following-sibling::td/span/text()')
        return apn_result[0].replace(" ", "") if apn_result else ""

    def get_garage(self, source, keyword):
        item = ""
        tmp = source.xpath('//ul[@class="ds-home-fact-list"]/li[@class="ds-home-fact-list-item"]//span[text()="'+keyword+'"]/following-sibling::span/text()')
        if tmp:
            item = tmp[0]
        return item

    def get_contactname(self, source):
        agent_name = source.xpath('//ul[@class="ds-listing-agent-info"]/li[1]/span/text()')
        return agent_name[0] if agent_name else ""

    def get_contactphone(self, source):
        contact_phone = source.xpath('//li[@class="ds-listing-agent-info-text"]/text()')
        if not contact_phone:
            contact_phone = source.xpath('//ul[@class="ds-listing-agent-info"]/li[3]/text()')
            if not contact_phone:
                contact_phone = source.xpath('//ul[@class="ds-listing-agent-info"]/li[2]/text()')
                if not contact_phone:
                    contact_phone = source.xpath('//div[@class="cf-cnt-rpt-sig-info"]/span[@data-test-id="cf-contact-phone-number"]/text()')
                    if not contact_phone:
                        phone = source.xpath('//div[@class="ov-seller-lead-agent-phone-info"]/text()')
                        if phone:
                            _phone = re.match(r'.+call\s(\d+-\d+-\d+).+', phone[0])
                            if _phone:
                                contact_phone = [_phone.groups()[0]]
        return contact_phone[0] if contact_phone else ""

    def data_page(self):
        # wait = WebDriverWait(browser, 60)
        # wait.until(EC.presence_of_element_located((By.XPATH, '//ul[@class="media-stream"]/li/picture')))
        source = etree.HTML(self.html)
        # print("url------------%s" % self.detail['detail_url'])
        try:
            price = self.detail["price"] if "price" in self.detail.keys() else self.get_price(source)
            bedroom = self.detail["bedrooms"] if "bedrooms" in self.detail.keys() else self.get_bedroom(source)
            bathroom = self.detail["bathrooms"] if "bathrooms" in self.detail.keys() else self.get_bathroom(source)
            street = self.detail["streetAddress"] if "streetAddress" in self.detail.keys() else self.get_street(source)
            deal_type = self.get_dealtype(source)
            img_url = self.get_imgurl(source)
            living_sqft = self.detail["livingArea"] if "livingArea" in self.detail.keys() else self.get_livingsqft(source)
            comments = self.get_desc(source).replace("\n", "")
            agent = self.get_agent(source)
            # house_type = self.get_info_by_keyword(source, 'Type:')
            house_type = self.get_info_by_keyword(source, 'Type:')
            _housetype = house_type if house_type else source.xpath( '//div[@class="home-facts-at-a-glance-section"]//div[contains(text(), "Type")]/following-sibling::div/text()')
            house_type = self.detail["homeType"] if "homeType" in self.detail.keys() else _housetype
            heating = self.get_heating(source)
            cooling = self.get_cooling(source)
            price_sqft = self.get_pricesqft(source)
            # Year built
            year_build = str(self.detail["yearBuilt"]) if "yearBuilt" in self.detail.keys() else self.get_info_by_keyword(source, 'Year built:')
            if not year_build or year_build == "-1":
                year_build = self.get_yearbuild(source)
                if not year_build:
                    year_build = source.xpath('//div[@class="home-facts-at-a-glance-section"]//div[contains(text(), "Year Built")]/following-sibling::div/text()')
            parking = self.get_parking(source)
            lot_sqft = self.detail["lotSize"] if "lotSize" in self.detail.keys() else self.get_lotsqft(source)
            hoa_fee = self.get_hoafee(source)
            mls = self.get_mls(source)
            apn = self.get_apn(source)
            garage = self.get_garage(source, 'Parking:')
            deposit = self.get_info_by_keyword(source, 'Deposit & fees:')
            contact_phone = self.get_contactphone(source)
            # time_on_zillow = detail["timeOnZillow"] if "timeOnZillow" in detail.keys() else 0
            contact_name = self.get_contactname(source)
            data = Data(price, bedroom, bathroom, street, deal_type, img_url, living_sqft,
                        comments, agent, house_type, heating, cooling, price_sqft, year_build,
                        parking, lot_sqft, hoa_fee, contact_phone, contact_name, "", self.detail['detail_url'], mls, apn, garage, deposit, self.detail["zipcode"], self.detail["latitude"], self.detail["longitude"], self.detail["city"], self.detail["state"])
            self.re_queue.sadd("house_data", data.dict2str())
        except Exception as e:
            print(e)


class DataOpt(object):
    USER_AGENTS = [
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0."
        "30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
        "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727"
        "; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
        "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET"
        " CLR 3.0.04506.30)",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora"
        "/0.3 (Change: 287 c9dfb30)",
        "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
        "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5"
    ]

    headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/web'
                         'p,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
               'accept-encoding': 'gzip, deflate, br',
               'accept-language': 'zh-CN,zh;q=0.9',
               'cache-control': 'max-age=0',
               'upgrade-insecure-requests': '1',
               'user-agent': random.choice(USER_AGENTS)}

    def __init__(self, filname, mode, host, port, db):
        # self.ip = requests.get('https://dps.kdlapi.com/api/getdps/?orderid=916161680251769&num=1&area=%E5%B9%BF%E4%B8%9C%2C%E7%A6%8F%E5%BB%BA%2C%E6%B5%99%E6%B1%9F%2C%E6%B1%9F%E8%A5%BF%2C%E5%8C%97%E4%BA%AC%2C%E6%B9%96%E5%8D%97%2C%E9%A6%99%E6%B8%AF%2C%E4%BA%91%E5%8D%97%2C%E5%A4%A9%E6%B4%A5&pt=1&dedup=1&sep=1&signature=hlmmo7aeico6vfnt0fzrljg84txca7zw').text
        self.driver_url = codecs.open(filname, mode, encoding="utf-8").read()
        self.re_queue = redis.Redis(host=host, port=port)
        self.detail_queue = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        # self.robots_flag = False

    def get_ip(self):
        if self.re_queue.scard("proxy_ip") < 3:
            _ip = requests.get('https://dps.kdlapi.com/api/getdps/?orderid=916161680251769&num=1&area=%E5%B9%BF%E4%B8%9C%2C%E7%A6%8F%E5%BB%BA%2C%E6%B5%99%E6%B1%9F%2C%E6%B1%9F%E8%A5%BF%2C%E5%8C%97%E4%BA%AC%2C%E6%B9%96%E5%8D%97%2C%E9%A6%99%E6%B8%AF%2C%E4%BA%91%E5%8D%97%2C%E5%A4%A9%E6%B4%A5&pt=1&dedup=1&sep=1&signature=hlmmo7aeico6vfnt0fzrljg84txca7zw').text
            self.re_queue.sadd("proxy_ip", _ip)
            self.ip = _ip
        else:
            # 返回1个随机数
            self.ip = self.re_queue.srandmember("proxy_ip", 1)[0].decode()

    def star_chr(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--disable-gpu')
        # prefs = {"profile.managed_default_content_settings.images": 2}
        # chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        time.sleep(5)
        browser = webdriver.Chrome(executable_path=self.driver_url, chrome_options=chrome_options)
        return browser

    """"
    功能：如果出现验证机器人，则使用更换代理的方式
    """
    def star_proxy_chr(self):
        chrome_options = webdriver.ChromeOptions()
        print("当前IP是: %s" % self.ip)
        proxy = "--proxy-server=http://" + self.ip
        chrome_options.add_argument(proxy)
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--disable-gpu')
        # prefs = {"profile.managed_default_content_settings.images": 2}
        # chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        time.sleep(5)
        browser = webdriver.Chrome(executable_path=self.driver_url, chrome_options=chrome_options)
        return browser

    def origin_page(self, url, browser):
        # url = detail["detail_url"]
        browser.get(url)
        html = browser.page_source
        if 'robots' in html:
            time.sleep(80)
            raise Exception("robots")
        return html

    def open_zil(self, url):
        self.star_chr.get(url)  # 打开网页
        time.sleep(1)
        html = self.star_chr.page_source
        if '无法访问此网站' in html or 'robot' in html or '未连接到互联网' in html:
            time.sleep(10)
        else:
            json_list = etree.HTML(self.star_chr.page_source).xpath('//pre/text()')
            if json_list:
                return "".join(json_list)
        return ""

    def get_element(self):
        result = self.detail_queue.spop("content")
        if result:
            _json = json.loads(result)
            time.sleep(5)
            return (_json, result)
        return ()


# def main():
#     dataopt = DataOpt('C:/devtools/chrome_driver.txt', 'rb', '47.106.140.94', '6486', 2)
#     # dataopt = DataOpt('E:/工作日常文档/爬虫/crawl_driver/chrome_driver.txt', 'rb', '47.106.140.94', '6486', 2)
#     while dataopt.detail_queue.scard("content"):
#         res = dataopt.get_element()
#         if res:
#             _json, result = res
#             browser = None
#             url = _json['detail_url']
#             try:
#                 browser = dataopt.star_chr()
#                 html = dataopt.origin_page(url, browser)
#                 page_deal = PageDeal(_json, html, dataopt.re_queue)
#                 page_deal.data_page()
#                 time.sleep(10)
#             except Exception as e:
#                 info = e.args[0]
#                 if info == 'robots':
#                     dataopt.robots_flag = True
#                     dataopt.get_ip()
#                     dataopt.detail_queue.sadd("content", result)
#             finally:
#                 browser.quit()
#         else:
#             raise Exception("finsh")

def produce(dataopt):
    # 先在消息队列中放入200个初始产品
    print("main thread start")
    total = dataopt.detail_queue.scard("content")
    for i in range(1, 6):
        result = dataopt.detail_queue.spop("content")
        if result:
            _json = json.loads(result)
            # dataopt.get_ip()
            browser = dataopt.star_chr()
            try:
                url = _json['detail_url']
                html = dataopt.origin_page(url, browser)
                que.put((url, html, _json))
            except Exception as e:
                dataopt.get_ip()
                dataopt.detail_queue.sadd("content", result)
            finally:
                browser.quit()
    print("predict redis-db2 take url %d, redis-db2 less %d requests" % (que.qsize(), total))


def main():
    # dataopt = DataOpt('C:/devtools/chrome_driver.txt', 'rb', '47.106.140.94', '6486', 2)
    dataopt = DataOpt('E:/工作日常文档/爬虫/crawl_driver/chrome_driver.txt', 'rb', '47.106.140.94', '6486', 2)
    # 启动生产者线程
    for i in range(5):
        # 启动消费者线程
        p = Producer(dataopt)
        p.start()
    # 这里休眠一秒钟，等到队列有值，否则队列创建时是空的，主线程直接就结束了，实验失败，造成误导
    time.sleep(5)
    for i in range(2):
        # 启动消费者线程
        c1 = Consumer(dataopt)
        c1.start()
    global que
    # 接收信号，主线程在这里等待队列被处理完毕后再做下一步
    que.join()
    # 给个标示，表示主线程已经结束


if __name__ == '__main__':
        main()

