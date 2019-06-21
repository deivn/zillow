
import time
import json
import requests
import redis
import codecs
import random
ip = '58.218.200.226:5983'

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


def ip_mit():
    return requests.get('http://dps.kdlapi.com/api/getdps/?orderid=926084328639054&num=1&area=%E5%B9%BF%E4%B8%9C%2C%E7%A6%8F%E5%BB%BA%2C%E6%B5%99%E6%B1%9F%2C%E6%B1%9F%E8%A5%BF%2C%E5%8C%97%E4%BA%AC%2C%E6%B9%96%E5%8D%97%2C%E9%A6%99%E6%B8%AF%2C%E4%BA%91%E5%8D%97%2C%E5%A4%A9%E6%B4%A5&pt=1&sep=1').text


if __name__ == '__main__':
    f = codecs.open("req.log", "w", encoding="utf-8")
    re_queue = redis.Redis(host='47.106.140.94', port='6486')
    # 详情页爬取
    detail_queue = redis.Redis(host='47.106.140.94', port='6486', db=2, decode_responses=True)
    # 详情页去重
    repeat_queue = redis.Redis(host='47.106.140.94', port='6486', db=3, decode_responses=True)
    while True:
        result = re_queue.rpop("params_info")
        if not result:
            f.close()
            print('队列任务为空，休眠15分钟')
            time.sleep(900)
        else:
            urls = json.loads(result)['urls']
            while True:
                # browser = star_chr()
                url = urls + '\n'
                f.write(url)
                if not urls:
                    break
                proxies = {'http': 'http://' + ip}
                json_res = requests.get(urls, headers=headers, proxies=proxies).text
                flag = False
                # json_res = open_zil(browser, urls)
                if not json_res:
                    ip = ip_mit()
                    proxies = {'http': 'http://' + ip}
                    json_res = requests.get(urls, headers=headers, proxies=proxies).text
                # browser.quit()
                if not json_res:
                    break
                _json = None
                try:
                    _json = json.loads(json_res)
                except Exception as e:
                    print("------json_res: %s" % json_res)

                if 'list' in _json.keys():
                    if 'numPages' in _json['list'].keys():
                        flag = True
                if flag:
                    list_house = _json['map']['properties']
                    if list_house:
                        for j in range(len(list_house)):
                            dict_h = list_house[j][8][11]
                            try:
                                zipcode = dict_h['zipcode']
                            except Exception as e:
                                print("dict------------%s" % dict_h)
                            zpid = dict_h['zpid']
                            latitude = dict_h['latitude']
                            longitude = dict_h['longitude']
                            city = dict_h['city']
                            state = dict_h['state']
                            detail_url = 'https://www.zillow.com/homedetails/' + str(zpid) + '_zpid'
                            # info = {"urls": i}
                            details = {"url": detail_url, "detail": {"state": state, "city": city, "zipcode": zipcode, "latitude": latitude, "longitude": longitude}}
                            content = json.dumps(details, ensure_ascii=False)
                            # SISMEMBER key member, 先去判断是否存在db3中，如果不存在，则添加，否则视为重复
                            # 这样就可以保证 db2中的元素是不重复的   db3用来去重      db2用来爬取
                            flag = repeat_queue.sismember("content", content)
                            if not flag:
                                repeat_queue.sadd("content", content)
                                detail_queue.sadd("content", content)
                        print('本区域总计 %d 个详情页面' % len(list_house))
                    break
                else:
                    break
