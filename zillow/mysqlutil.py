#! /usr/bin/env python  
# -*- coding:utf-8 -*-
import MySQLdb
from scrapy.conf import settings
import re
"""
scrapy.log.CRITICAL
严重错误的 Log 级别

scrapy.log.ERROR
错误的 Log 级别 Log level for errors

scrapy.log.WARNING
警告的 Log 级别 Log level for warnings

scrapy.log.INFO
记录信息的 Log 级别(生产部署时推荐的 Log 级别)

scrapy.log.DEBUG
调试信息的 Log 级别(开发时推荐的 Log 级别)
"""


class MysqlHelper(object):
    host = settings['MYSQL_HOST']
    port = settings['MYSQL_PORT']
    db = settings['MYSQL_DB']
    user = settings['MYSQL_USER']
    passwd = settings['MYSQL_PASSWD']
    charset = 'utf8'

    def __init__(self):
        pass


    @classmethod
    def connect(cls):
        cls.conn = MySQLdb.connect(host=cls.host, port=cls.port, db=cls.db, user=cls.user, passwd=cls.passwd, charset=cls.charset)
        cls.cursor = cls.conn.cursor()

    @staticmethod
    def close():
        MysqlHelper.cursor.close()
        MysqlHelper.conn.close()

    @staticmethod
    def get_one(sql, params=()):
        return MysqlHelper._find_one(sql, params)

    @staticmethod
    def get_all(sql, param=()):
        return MysqlHelper()._find_all(sql, param)

    @classmethod
    def _find_all(cls, sql, param):
        list = ()
        try:
            cls.connect()
            cls.cursor.execute(sql, param)
            list = cls.cursor.fetchall()
            # self.close()
        except Exception as e:
            print(e)
        return list

    @classmethod
    def _find_one(cls, sql, param):
        result = None
        try:
            cls.connect()
            cls.cursor.execute(sql, param)
            result = cls.cursor.fetchone()
            cls.close()
        except Exception as e:
            print(e)
        return result

    @staticmethod
    def insert(sql, params=()):
        return MysqlHelper.__edit(sql, params)

    @staticmethod
    def update(sql, params=()):
        return MysqlHelper.__edit(sql, params)

    @staticmethod
    def delete(sql, params=()):
        return MysqlHelper.__edit(sql, params)

    @classmethod
    def __edit(cls, sql, params):
        count = 0
        try:
            cls.connect()

            # 获取自增长的主键，一定要在commit之前，否则返回为0
            is_click_url_tab = re.search('click_url', sql)
            if is_click_url_tab:
                cls.cursor.execute(sql, params)
                count = cls.conn.insert_id()
            else:
                count = cls.cursor.execute(sql, params)
            cls.conn.commit()
        except Exception as e:
            print(e)
            # log.msg(e.message, log.ERROR)
        return count

    @staticmethod
    def _get_insert_id():
        return MysqlHelper.conn.insert_id()
