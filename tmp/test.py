#! /usr/bin/env python  
# -*- coding:utf-8 -*-
import codecs
import re
import time


def main(key, delimter, fileName):
    f = open(fileName, 'r')
    info = f.read()
    f.close()
    lines = re.findall('%s=\d+.+' % key, info)
    delimiter_count = 0
    if len(lines):
        value = re.sub(key + "=", "", lines[0].strip()).split(delimter)
        if len(value) > 5:
            ti = time.strftime("%Y-%m-%d", time.localtime())
            # 新的日期在该行不存在，则先将首位的数据删除，再往该行末尾追加新的日期
            if ti not in value:
                del value[0]
                value.append(ti)
                with codecs.open(fileName, 'wb') as f1:
                    # 1.将匹配到的行数据替换
                    info = info.replace(lines[0], key+'='+delimter.join(value)+'\n')
                    tmp = info.encode()
                    # 2.替换后数据重新写入
                    f1.write(tmp)
        delimiter_count = len(value) - 1
    print("最终统计的指定字符%s个数是: %d" % (delimter, delimiter_count))


if __name__ == "__main__":
    main('yu', ',', 't.txt')