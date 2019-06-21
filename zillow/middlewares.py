# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import random
import time
import codecs
import base64
import ssl
import string
from urllib import request
from urllib.parse import quote
from selenium import webdriver
from scrapy import signals
from scrapy.conf import settings
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.common.by import By
from lxml import etree
import requests
import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from scrapy.downloadermiddlewares.retry import RetryMiddleware
from twisted.internet import defer
from twisted.internet.error import TimeoutError, DNSLookupError, \
    ConnectionRefusedError, ConnectionDone, ConnectError, \
    ConnectionLost, TCPTimedOutError
from twisted.web.client import ResponseFailed
from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.utils.response import response_status_message
from scrapy.http import TextResponse

class RandomUserAgent(object):
    def __init__(self, agents):
        self.agents = agents

    @classmethod
    def from_crawler(cls, crawler):
        # 加载IPLIST
        return cls(settings['USER_AGENTS'])

    # 该方法在引擎发送http request请求给下载器时会经过下载中间件，可以设置ip代理，User-Agent, 设置关闭cookie
    def process_request(self, request, spider):
        """
        从配置文件读取user-agent池中的数据，每次请求都随机选一个user-agent
        :param request: 在引擎发送给下载器的http request请求
        :param spider: 引擎
        :return:
        """
        useragent = random.choice(self.agents)
        request.headers['User-Agent'] = useragent


class RandomProxy(object):
    def __init__(self):
        self.proxy = self.get_ip_proxy()

    # @classmethod
    # def from_crawler(cls):
    #     return cls(settings['PROXY_URL'])

    def process_request(self, request, spider):
        if self.proxy:
            if self.proxy['user_pass']:
                # 参数是bytes对象,要先将字符串转码成bytes对象
                encoded_user_pass = base64.b64encode(self.proxy['user_pass'].encode('utf-8'))
                request.headers['Proxy-Authorization'] = 'Basic ' + str(encoded_user_pass, 'utf-8')
                request.meta['proxy'] = "http://" + self.proxy['ip_port']
            else:
                request.meta['proxy'] = "http://" + self.proxy['ip_port']

    def get_ip_proxy(self):
        url = 'https://dps.kdlapi.com/api/getdps/?orderid=926084328639054&num=1&area=%E5%B9%BF%E4%B8%9C%2C%E7%A6%8F%E5%BB%BA%2C%E6%B5%99%E6%B1%9F%2C%E6%B1%9F%E8%A5%BF%2C%E5%8C%97%E4%BA%AC%2C%E6%B9%96%E5%8D%97%2C%E9%A6%99%E6%B8%AF%2C%E4%BA%91%E5%8D%97%2C%E5%A4%A9%E6%B4%A5&pt=1&sep=1&signature=z703jibe99m7y6t5hi9y7r98tu9c4w5x'
        ssl._create_default_https_context = ssl._create_unverified_context
        result = request.urlopen(quote(url, safe=string.printable))
        try:
            info = result.read().decode(encoding='utf-8')
            if info:
                self.proxy = {"ip_port": info, "user_pass": ""}
        except Exception as e:
            print("exception info: %s" % e)
        return self.proxy


class PhantomJSMiddleware(object):
    # def __init__(self, path, chrome_options):
    #     self.driver = webdriver.Chrome(executable_path=path, chrome_options=chrome_options)
    #
    @classmethod
    def from_crawler(cls, crawler):
        cls.get_ip()
    #     chrome_options = webdriver.ChromeOptions()

    #     proxy = "--proxy-server=http://" + cls.ip
    #     print('当前IP为：', cls.ip)
    #     chrome_options.add_argument(proxy)
    #     chrome_options.add_argument('--ignore-certificate-errors')
    #     chrome_options.add_argument('--disable-gpu')
    #     prefs = {"profile.managed_default_content_settings.images": 2}
    #     chrome_options.add_experimental_option("prefs", prefs)
    #     chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
    #     time.sleep(1)
    #     return cls(settings['GOOGLE_DRIVER_URL'], chrome_options)

    @classmethod
    def get_ip(cls):
        cls.ip = requests.get('https://dps.kdlapi.com/api/getdps/?orderid=926084328639054&num=1&area=%E5%B9%BF%E4%B8%9C%2C%E7%A6%8F%E5%BB%BA%2C%E6%B5%99%E6%B1%9F%2C%E6%B1%9F%E8%A5%BF%2C%E5%8C%97%E4%BA%AC%2C%E6%B9%96%E5%8D%97%2C%E9%A6%99%E6%B8%AF%2C%E4%BA%91%E5%8D%97%2C%E5%A4%A9%E6%B4%A5&pt=1&sep=1&signature=z703jibe99m7y6t5hi9y7r98tu9c4w5x').text

    def process_request(self, request, spider):
        # flag = request.meta.get('PhantomJS')
        if spider.name == 'zillow':
            chrome_options = webdriver.ChromeOptions()
            proxy = "--proxy-server=http://" + self.ip
            print('当前IP为：', self.ip)
            chrome_options.add_argument(proxy)
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--disable-gpu')
            prefs = {"profile.managed_default_content_settings.images": 2}
            chrome_options.add_experimental_option("prefs", prefs)
            chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
            time.sleep(1)
            self.driver = webdriver.Chrome(executable_path=settings['GOOGLE_DRIVER_URL'], chrome_options=chrome_options)
            self.driver.get(request.url)
            wait = WebDriverWait(self.driver, 60)
            body = None
            try:
                wait.until(EC.presence_of_element_located((By.XPATH, '//ul[@class="media-stream"]/li/picture')))
                body = etree.HTML(self.driver.page_source)
            except Exception as e:
                print("exception-------------------------------------%s" % e)
                print('页面访问超时，准备更换IP重试')
                time.sleep(10)
                self.get_ip()
            finally:
                self.driver.quit()
            # self.driver.close()
            # driver.save_screenshot('1.png')
            return HtmlResponse(request.url, body=body, encoding='utf-8', request=request)
        else:
            return

    def process_response(self, request, response, spider):
        return response

    def process_exception(self, request, exception, spider):
        return request


class ProcessAllExceptionMiddleware(RetryMiddleware):

    ALL_EXCEPTIONS = (defer.TimeoutError, TimeoutError, DNSLookupError,
                      ConnectionRefusedError, ConnectionDone, ConnectError,
                      ConnectionLost, TCPTimedOutError, ResponseFailed,
                      IOError, TunnelError)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        #  or response.status == 301
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            time.sleep(random.randint(3, 5))
            # 更换代理
            self.set_ip_proxy()
            self.set_proxy(request, spider)
            print('更换后的代理是: %s' % (request.meta['proxy']))
            return self._retry(request, reason, spider) or response
        if response.status == 307 or response.status == 301:
            self.exist_proxy()
            response = TextResponse(url=request.url, status=200, request=request, body=self.get_page_resource(request))
            # response['info'] = self.get_page_resource(request)
        return response

    def exist_proxy(self):
        try:
            self.proxy["ip_port"]
        except Exception as e:
            self.set_ip_proxy()

    def get_page_resource(self, request):
        chrome_options = webdriver.ChromeOptions()
        proxy = "--proxy-server=http://" + self.proxy["ip_port"]
        chrome_options.add_argument(proxy)
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--disable-gpu')
        prefs = {"profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
        time.sleep(1)
        self.driver = webdriver.Chrome(executable_path=settings['GOOGLE_DRIVER_URL'], chrome_options=chrome_options)
        self.driver.get(request.url)
        wait = WebDriverWait(self.driver, 30)
        body = None
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, '//ul[@class="media-stream"]/li/picture')))
            body = self.driver.page_source.encode('utf-8')
        except Exception as e:
            print("exception-------------------------------------%s" % e)
            print('页面访问超时，准备更换IP重试')
            self.set_ip_proxy()
        finally:
            self.driver.quit()
        return body

    def process_exception(self, request, exception, spider):
        # 设置同一个代理发生异常的次数
        # 捕获几乎所有的异常
        if isinstance(exception, self.ALL_EXCEPTIONS):
            # 在日志中打印异常类型
            print('Got exception: %s' % (exception))
            # self.del_proxy(request.meta.get('proxy', False))
            time.sleep(random.randint(3, 5))
            # 设置新的代理
            self.set_ip_proxy()
            self.set_proxy(request, spider)
            # 随意封装一个response，返回给spider
            return self._retry(request, exception, spider)

    def set_proxy(self, request, spider):
        if self.proxy:
            if self.proxy['user_pass']:
                # 参数是bytes对象,要先将字符串转码成bytes对象
                encoded_user_pass = base64.b64encode(self.proxy['user_pass'].encode('utf-8'))
                request.headers['Proxy-Authorization'] = 'Basic ' + str(encoded_user_pass, 'utf-8')
                request.meta['proxy'] = "http://" + self.proxy['ip_port']
            else:
                request.meta['proxy'] = "http://" + self.proxy['ip_port']

    '''通过接口获取最新的代理'''
    def set_ip_proxy(self):
        url = 'https://dps.kdlapi.com/api/getdps/?orderid=926084328639054&num=1&area=%E5%B9%BF%E4%B8%9C%2C%E7%A6%8F%E5%BB%BA%2C%E6%B5%99%E6%B1%9F%2C%E6%B1%9F%E8%A5%BF%2C%E5%8C%97%E4%BA%AC%2C%E6%B9%96%E5%8D%97%2C%E9%A6%99%E6%B8%AF%2C%E4%BA%91%E5%8D%97%2C%E5%A4%A9%E6%B4%A5&pt=1&sep=1&signature=z703jibe99m7y6t5hi9y7r98tu9c4w5x'
        ssl._create_default_https_context = ssl._create_unverified_context
        result = request.urlopen(quote(url, safe=string.printable))
        try:
            info = result.read().decode(encoding='utf-8')
            if info:
                print("当前IP代理为:%s" % info)
                self.proxy = {"ip_port": info, "user_pass": ""}
        except Exception as e:
            print("exception info: %s" % e)
        return self.proxy


class ZillowMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class ZillowDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
