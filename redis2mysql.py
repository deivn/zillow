#! /usr/bin/env python  
# -*- coding:utf-8 -*-
import json
import redis
import MySQLdb
import re
import random
import pandas as pd


class Redis2Mysql(object):
    def __init__(self, city_path, housetype_path):
        self._redis = redis.Redis(host='47.106.140.94', port='6486')
        self.mysqlcli = MySQLdb.connect(host='120.78.196.201', user='ebuyhouse', passwd='ebuyhouse', db='crawl', port=3306, use_unicode=True, charset='utf8')
        # 读取Excel文件
        self.city_df = pd.read_excel(city_path)
        # 创建最终返回的结果
        self.city_list = []
        self.load_city_excel()
        # -------------------house_type------------------
        self.housetype_df = pd.read_excel(housetype_path)
        self.housetypes = []
        self.load_housetypes()

    # 加载city配置
    def load_city_excel(self):
        # 替换Excel表格内的空单元格，否则在下一步处理中将会报错
        self.city_df.fillna("", inplace=True)
        # 结果集
        for i in self.city_df.index.values:
            # loc为按列名索引 iloc 为按位置索引，使用的是 [[行号], [列名]]
            df_line = self.city_df.loc[i, ['city_name', 'state_id', 'id', ]].to_dict()
            # 将每一行转换成字典后添加到列表
            self.city_list.append(df_line)

    # 加载house_types
    def load_housetypes(self):
        # 替换Excel表格内的空单元格，否则在下一步处理中将会报错
        self.housetype_df.fillna("", inplace=True)
        for j in self.housetype_df.index.values:
            df_line = self.housetype_df.loc[j, ["id", "name", ]].to_dict()
            self.housetypes.append(df_line)

    def get_houseid(self, street):
        return "ebh-" + street.replace("#", "").replace(" ", "-").replace(",", "-").lower()

    def get_cid(self, state, city):
        if self.city_list:
            for city_dic in self.city_list:
                city_name = city_dic['city_name']
                state_id = city_dic['state_id']
                cid = city_dic['id']
                if state == state_id and city == city_name:
                    return cid
        return ""

    def get_housetypeid(self, housetype_name):
        type = housetype_name.replace(" ", "").replace("_", "").lower()
        if self.housetypes:
            for housetype_dic in self.housetypes:
                id = housetype_dic['id']
                name = housetype_dic['name'].replace(" ", "").lower()
                if type == name or type in name:
                    return id
        return ""

    def get_garage(self, garage):
        tmp = 0
        if garage and garage != 'No':
            _garage = re.findall(r'\d+', garage)
            if _garage:
                tmp = int(_garage[0])
        return tmp

    """"
    功能： 获取图片，如果不包含谷歌地图图片，则判断，如果图片只有一张，主图辅图共用同一张。如果多张，则取第一张为主图，剩余为辅图
    如果包含谷歌图片，则将包含的谷歌图片删除，取剩余，逻辑同上
    """
    def get_img(self, imges, rex):
        if imges:
            if rex in imges:
                _imgs = imges.split(",")
                for img in _imgs:
                    if rex in img:
                        _imgs.remove(img)
                return self.img_deal(_imgs)
            else:
                return self.img_deal(imges)
        return ()

    """"功能：如果图片只有一张，则主图和辅图同为一张，如果为多张，则取第一张为主图，剩余为辅图"""
    def img_deal(self, imgs):
        # 列表
        if not isinstance(imgs, str):
            img_len = len(imgs)
            if img_len:
                if img_len == 1:
                    return (imgs[0], imgs[0])
                else:
                    imges = ",".join(imgs)
                    first = imges[:imges.find(',')]
                    less = imges[imges.find(',') + 1:]
                    return (first, less)
        else:
            # 字符串不包含谷歌图片并且长度大小为1和大余1的情况
            _imgs = imgs.split(",")
            if len(_imgs) == 1:
                return (_imgs[0], _imgs[0])
            first = imgs[:imgs.find(',')]
            less = imgs[imgs.find(',') + 1:]
            return (first, less)
        return ()

    def get_dealtype(self, dealtype):
        _dealtype = dealtype.lower()
        # 1 出售  2 整租 3 合租
        if "rent" in _dealtype:
            return 2
        elif "sale" in _dealtype:
            return 1

    def get_deposit(self, deposit):
        if deposit and deposit != 'No Data':
            _deposit = deposit.replace('$', '').replace(',', '')
            return _deposit
        return "0"


def main():
    redis2mysql = Redis2Mysql("city.xlsx", "house_type.xlsx")
    while True:
        # FIFO模式为 blpop，LIFO模式为 brpop，获取键值
        data = redis2mysql._redis.spop("data")
        _item = json.loads(data)
        # 使用cursor()方法获取操作游标
        cur = redis2mysql.mysqlcli.cursor()
        item = {}
        imgs = redis2mysql.get_img(_item['img_url'], 'maps.googleapis.com')
        contact_phone = _item['contact_phone'] if _item['contact_phone'] else ""
        # 不含图片，不含手机号则不入库
        if imgs and contact_phone:
            # 使用execute方法执行SQL INSERT语
            item['house_id'] = redis2mysql.get_houseid(_item['street'])
            item['user_id'] = '20190629'
            item['state_id'] = _item['state']
            item['city_id'] = redis2mysql.get_cid(_item['state'], _item['city'])
            item['house_type_id'] = redis2mysql.get_housetypeid(_item['house_type'])
            item['price'] = _item['price']
            item['hoa_fee'] = _item['hoa_fee'] if _item['hoa_fee'] and _item['hoa_fee'] != 'No' else "0"
            item['mls'] = _item['mls'] if _item['mls'] and _item['mls'] != 'No' else ""
            item['apn'] = _item['apn'] if _item['apn'] and _item['apn'] != 'No' else ""
            item['street'] = _item['street']
            item['zip'] = _item['zip']
            item['bedroom'] = _item['bedroom']
            item['bathroom'] = _item['bathroom']
            item['garage'] = redis2mysql.get_garage(_item['garage'])
            item['lot_sqft'] = _item['lot_sqft'] if _item['lot_sqft'] and _item['lot_sqft'] != -1.0 else 0
            item['user_input_unit'] = str(item['lot_sqft']) + ' sqft'
            item['living_sqft'] = _item['living_sqft'] if _item['living_sqft'] and _item['living_sqft'] != -1.0 else 0
            item['latitude'] = _item['latitude']
            item['longitude'] = _item['longitude']
            item['year_build'] = _item['year_build'] if _item['year_build'] else ""
            first, less = imgs
            item['img_url'] = first
            item['deal_type'] = redis2mysql.get_dealtype(_item['deal_type'])
            item['deposit'] = redis2mysql.get_deposit(_item['deposit'])
            item['contact_name'] = _item['contact_name'] if _item['contact_name'] else ""
            item['contact_phone'] = contact_phone
            item['contact_email'] = '20190629@gmail.com'
            item['create_time'] = random.randint(1560480030, 1561891667)
            item['url'] = _item['url']
            # -------------------------------house_detail----------------
            item['house_img'] = less
            item['house_desc'] = _item['comments']
            try:
                count = cur.execute("INSERT ignore t_houses_new (\
                            house_id, user_id, state_id, city_id, house_type_id, price, hoa_fee, mls, apn, street, \
                            zip, bedroom, bathroom, garage, lot_sqft, user_input_unit, living_sqft, latitude,longitude, \
                            year_build, img_url, origin, garage_sqft, deal_type, rent_payment, check_status, shelf_status, basement_sqft, transaction_status, \
                            deposit, contact_name, contact_phone, contact_email, email_conceal, create_time, decorate_grade, count_share, enable_status, advantage, property_tax, url) VALUES (\
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            [item['house_id'], item['user_id'], item['state_id'], item['city_id'], item['house_type_id'], item['price'], item['hoa_fee'], item['mls'], item['apn'], item['street'],
                             item['zip'], item['bedroom'], item['bathroom'], item['garage'], item['lot_sqft'], item['user_input_unit'], item['living_sqft'], item['latitude'], item['longitude'],
                            item['year_build'], item['img_url'], 1, 0, item['deal_type'], 1, 1, 1, 0, 1,
                            item['deposit'], item['contact_name'], item['contact_phone'], item['contact_email'], 2, item['create_time'], "", 0, 1, "", 0, item['url']])
                if count == 1:
                    cur.execute("INSERT ignore t_house_detail_new (house_id, house_img, house_video, house_desc, house_deed, house_amenities, deny_reason, view_count, house_amenities_value) VALUES (\
                                %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                [item['house_id'], item['house_img'], "", item['house_desc'], "", "", "", 0, ""])
                    print("inserted %s" % item['url'])
            except Exception as e:
                print(e)
                cur.execute("delete from t_houses_new where house_id = %s",[item['house_id']])
            finally:
                # 提交sql事务
                redis2mysql.mysqlcli.commit()
                # 关闭本次操作
                cur.close()
        else:
            continue


if __name__ == '__main__':
    main()