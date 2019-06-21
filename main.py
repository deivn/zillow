#! /usr/bin/env python  
# -*- coding:utf-8 -*-
import redis
import json
import re
import time
import requests
import random

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


def segmentation(list_tude, rid, zoom):
    url_zillow = []
    num_y = list_tude[1] - list_tude[3] + 1
    num_x = list_tude[2] - list_tude[0] + 1
    lists = []
    for y in range(0, num_y, num_y//6):
        for x in range(0, num_x, num_x//6):
            ys = y + list_tude[3]
            xs = list_tude[2] - x
            lists.append([ys, xs])
    for i in lists:
        site = '-' + str(i[0]) + '0000,' + str(i[1]) + '0000,-' + str(i[0] - num_y//6) + '0000,' + str(i[1] + num_x//6) + '0000'
        url_str = 'https://www.zillow.com/search/GetResults.htm?spt=homes&status=110001&lt=111101&ht=111111&pr=,&mp=,' \
                  '&bd=0%2C&ba=0%2C&sf=,&lot=0%2C&yr=,&singlestory=0&hoa=0%2C&pho=0&pets=0&parking=0&laundry=0&income-' \
                  'restricted=0&fr-bldg=0&condo-bldg=0&furnished-apartments=0&cheap-apartments=0&studio-apartments=0' \
                  '&pnd=0&red=0&zso=0&days=any&ds=all&pmf=1&pf=1&sch=100111&zoom='+zoom+'&rect=' + site + \
                  '&p=1&sort=days&search=maplist'
        if rid:
            url_str += '&rid='+rid+'&rt=2'
        url_str += '&listright=true&isMapSearch=true&zoom='+ zoom
        url_zillow.append(url_str)
    return url_zillow


def cut_area(urls):
    jwd_int = []
    # 分组
    g = re.match(
        r'^https://www.zillow.com/search/GetResults.htm\?spt=homes&status=000010&lt=000000&ht=111111.+&rect=([-]?\d+,[-]?\d+,[-]?\d+,[-]?\d+)',
        urls)
    if g.groups():
        jwd = g.groups()[0].replace('-', '').split(',')
        jwd[1], jwd[0] = jwd[0], jwd[1]
        jwd[2], jwd[3] = jwd[3], jwd[2]
        for i in jwd:
            jwd_int.append(int(i[:-4]))
        jwd_int[0] = jwd_int[0] - 25
        jwd_int[1] = jwd_int[0] + 25
        # 出租rid=42&rt=2
        zoom_match = re.match(r'^https://www.zillow.com/search/GetResults.htm\?spt=homes&status=000010&lt=000000.+&zoom=(\d+)', urls)
        zoom = zoom_match.groups()[0]
        rid_match = re.match(r'^https://www.zillow.com/search/GetResults.htm\?spt=homes&status=000010&lt=000000.+&rid=(\d+)', urls)
        if rid_match.groups():
            rid = rid_match.groups()[0]
            # 每一次的网络请求
            max_list = segmentation(jwd_int, rid, zoom)
        else:
            max_list = segmentation(jwd_int, '', zoom)
        # print('切割完毕，准备加入队列')
        for i in max_list:
            params_dict = {"urls": i}
            # 从左往右入队到redis
            req_queue.lpush("params_info", json.dumps(params_dict))
        print('插入队列完成，本次插入', len(max_list), '个任务')
def ip_mit():
    return requests.get('http://dps.kdlapi.com/api/getdps/?orderid=926084328639054&num=1&area=%E5%B9%BF%E4%B8%9C%2C%E7%A6%8F%E5%BB%BA%2C%E6%B5%99%E6%B1%9F%2C%E6%B1%9F%E8%A5%BF%2C%E5%8C%97%E4%BA%AC%2C%E6%B9%96%E5%8D%97%2C%E9%A6%99%E6%B8%AF%2C%E4%BA%91%E5%8D%97%2C%E5%A4%A9%E6%B4%A5&pt=1&sep=1').text


if __name__ == '__main__':
    # db1   用来做数据请求切分     db2   用来存详情   db0 用来存数据爬取
    req_queue = redis.Redis(host='47.106.140.94', port='6486', db=1, decode_responses=True)
    while True:
        result = req_queue.rpop("params_info")
        if not result:
            print('队列任务为空，休眠15分钟')
            time.sleep(900)
        else:
            urls = json.loads(result)['urls']
            while True:
                proxies = {'http': 'http://' + ip_mit()}
                json_res = requests.get(urls, headers=headers, proxies=proxies).text
                if not json_res:
                    proxies = {'http': 'http://' + ip_mit()}
                    json_res = requests.get(urls, headers=headers, proxies=proxies).text
                if not json_res:
                    break
                _json = json.loads(json_res)
                if 'list' in _json.keys():
                    if 'numPages' in _json['list'].keys():
                        numPages = _json['list']['numPages']
                        if numPages > 20:
                            print('区域房源超过上限，二次切割, 并入队列')
                            cut_area(urls)
