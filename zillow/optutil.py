#! /usr/bin/env python  
# -*- coding:utf-8 -*-
import os
from urllib.parse import quote,unquote
from datetime import datetime
from scrapy.utils.project import get_project_settings


class OptUtil(object):

    @staticmethod
    def gen_file():
        dir = get_project_settings()['DATA_PATH_PREFIX']
        # 是否存在该目录
        is_exists = os.path.exists(dir)
        if not is_exists:
            os.makedirs(dir)
        today = datetime.now()
        full_path = dir + '/{}{}{}{}{}{}' + '.json'
        data_path = full_path.format(today.year, today.month, today.day, today.hour, today.minute, today.second)
        return data_path

    @staticmethod
    def urlDecoder(encoded_url):
        return unquote(encoded_url, 'utf-8')