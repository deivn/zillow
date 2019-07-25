#! /usr/bin/env python  
# -*- coding:utf-8 -*-
import MySQLdb
import random


def main():
    conn = MySQLdb.connect(host='ebuyhouse.ckzejnyrspwg.us-west-2.rds.amazonaws.com', user='ebuyhouse', passwd='63gPUqwm45a3aU', db='ebuyhouse_produce', port=3306, use_unicode=True, charset='utf8')
    cur = conn.cursor()
    try:
        # 详情表
        cur.execute("select id, create_time from t_houses_new where origin=%s", ['20190515'])
        # 房源主表
        # cur.execute("select id, img_url from t_houses_new0701")
        datas = cur.fetchall()
        if datas:
            count = 0
            for id, create_time in datas:
                time = random.randint(1388509385, 1561914185)
                cur.execute("update t_houses_new set create_time = %s where id = %s", [str(time), str(id)])
                conn.commit()
                count += 1
            print("共计更新记录%d条" % count)
    except Exception as e:
        print(e.args(0))
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()

