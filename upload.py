#! /usr/bin/env python  
# -*- coding:utf-8 -*-
import MySQLdb
import requests
import time
from multiprocessing import Lock
from urllib3 import encode_multipart_formdata


class SnowFlakeUtil(object):

    def __init__(self, workerId, datacenterId):
        self.worker_id_bits = 5
        self.max_worker_id = -1 ^ (-1 << self.worker_id_bits)
        self.data_center_id_bits = 5
        self.sequence_bits = 12
        self.twepoch = 1535990400000
        self.max_data_center_id = -1 ^ (-1 << self.data_center_id_bits)
        self.sequence_mask = -1 ^ (-1 << self.sequence_bits)
        self.timestamp_left_shift = self.sequence_bits + self.worker_id_bits + self.data_center_id_bits
        self.data_center_id_shift = self.sequence_bits + self.worker_id_bits
        self.worker_id_shift = self.sequence_bits
        self.worker_id = None
        self.data_center_id = None
        self.last_timestamp = -1
        self.sequence = 0
        if workerId > self.max_worker_id or workerId < 0:
            raise Exception("worker Id can't be greater than %d or less than 0" % self.max_worker_id)
        if datacenterId > self.max_data_center_id or datacenterId < 0:
            raise Exception("datacenter Id can't be greater than %d or less than 0", self.max_data_center_id)
        self.worker_id = workerId
        self.data_center_id = datacenterId

    def next_id(self):
        mutex = Lock()
        try:
            # 获取互斥锁后，进程只能在释放锁后下个进程才能进来
            mutex.acquire()
            timestamp = int(self.gen_time())
            if timestamp < self.last_timestamp:
                tmp = self.last_timestamp - timestamp
                raise Exception("Clock moved backwards.  Refusing to generate id for %d milliseconds" % tmp)
            if self.last_timestamp == timestamp:
                self.sequence = (self.sequence + 1) & self.sequence_mask
            if not self.sequence:
                timestamp = self.til_next_millis(self.last_timestamp)
            else:
                self.sequence = 0
            self.last_timestamp = timestamp
            return ((timestamp - self.twepoch) << self.timestamp_left_shift) | \
                   (self.data_center_id << self.data_center_id_shift) | \
                   (self.worker_id << self.worker_id_shift) | self.sequence
        except Exception as e:
            print(e)
        finally:
            # 互斥锁必须被释放掉
            mutex.release()

    def gen_time(self):
        timestamps = time.time()
        return str(timestamps).replace('.', '')

    def til_next_millis(self, last_time_stamp):
        timestamp = int(self.gen_time())
        while timestamp <= last_time_stamp:
            timestamp = self.gen_time()
        return timestamp


class ImageDeal(object):

    def __init__(self):
        pass

    def img_opt(self, *args):
        id, zillow_img_url = args
        try:
            ones = []
            img_urls = []
            if zillow_img_url.find(',') > -1:
                # 多张图片下载
                urls = zillow_img_url.split(',')
                if urls:
                    for url in urls:
                        # 下载图片
                        gen_num = SnowFlakeUtil(0, 0).next_id()
                        ones.append(self.download(url, gen_num))
            else:
                # 一张图片
                gen_num = SnowFlakeUtil(0, 0).next_id()
                one = self.download(zillow_img_url, gen_num)
                pcUrl = self.uploadReturnAmazonImg(*one)
                if pcUrl:
                    img_urls.append(pcUrl)
            if ones:
                # 上传多张图片
                for info in ones:
                    pcUrl = self.uploadReturnAmazonImg(*info)
                    if pcUrl:
                        img_urls.append(pcUrl)
            if img_urls:
                return id, img_urls
        except Exception as e:
            raise Exception(e)

    def uploadReturnAmazonImg(self, *info):
        try:
            url = "https://testadmin.ebuyhouse.com:8050/ebuyhouse-upload/api/uploadImages"
            # params = None, data = None, headers = None, cookies = None, files = None,
            headers = {}
            filename, file_data = info
            fields = {'multipartFiles': (filename, file_data, "image/jpeg"),}
            encode_data = encode_multipart_formdata(fields)
            data, content_type = encode_data
            headers['Content-Type'] = content_type
            res = requests.post(url, data=data, headers=headers).json()
            if "data" in res.keys():
                data_dic = res['data']
                pcUrl = data_dic['pcUrl'][0]
                return pcUrl
            else:
                return ''
        except Exception as e:
            raise Exception(e.args(0))

    def download(self, req_url, num):
        binary_data = requests.get(req_url).content
        img_suffix = req_url[req_url.rfind('.'):]
        new_name = str(num) + img_suffix
        # dir = "D:/工具/imgs/"+new_name
        # with codecs.open(dir, "wb") as f:
        #     f.write(binary_data)
        return new_name, binary_data


def main():
    conn = MySQLdb.connect(host='120.78.196.201', user='ebuyhouse', passwd='ebuyhouse', db='crawl', port=3306, use_unicode=True, charset='utf8')
    cur = conn.cursor()
    try:
        # 详情表
        cur.execute("select id, house_img from t_house_detail_new0718")
        # 房源主表
        # cur.execute("select id, img_url from t_houses_new0701")
        _imgs = cur.fetchall()
        if _imgs:
            imgDeal = ImageDeal()
            count = 0
            for id, img_url in _imgs:
                pos = img_url.find('zimg.ebuyhouse.com')
                if pos > 0:
                    orgin = img_url.replace('http://zimg.ebuyhouse.com', 'https://photos.zillowstatic.com')
                    args = (id, orgin)
                    res = imgDeal.img_opt(*args)
                    id, img_urls = res
                    urls = ','.join(img_urls)
                    print("id: %d, imgurls: %s" % (id, urls))
                    _count = cur.execute("update t_house_detail_new0718 set house_img = %s where id = %s", [urls, str(id)])
                    # _count = cur.execute("update t_houses_new0701 set img_url = %s where id = %s", [urls, str(id)])
                    conn.commit()
                    count += _count
            print("共计更新表图片记录%d条" % count)
    except Exception as e:
        print(e.args(0))
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()





