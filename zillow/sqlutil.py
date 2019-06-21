#! /usr/bin/env python  
# -*- coding:utf-8 -*-
from datetime import datetime


class SqlUtil(object):

    @classmethod
    def gen_sql_sql(self, tab_name, _fields):
        """
        用来生成sql字符串语句
        :param _fields:
        :return:
        """
        fields = ",".join(_fields)
        placeholders = []
        for i in range(0, len(_fields)):
            placeholders.append("%s")
        placeholder_str = ",".join(placeholders)
        sql = "insert ignore into %s (%s) values (%s)" % (tab_name, fields, placeholder_str)
        return sql