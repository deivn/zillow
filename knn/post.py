# "http://fanyi.youdao.com/translate_o?smartresult=dict&smartresult=rule"
from urllib import request
import urllib.parse
import re
import json
import requests
# key=input("请输入所要翻译的内容：")
url="http://api.hzmra.com:8011/web/seeDoctor/appointment/find/records"
# url="http://fanyi.youdao.com/translate?smartresult=dict&smartresult=rule"
# headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36",
#          "token": "e8f3071444ca5cc19120a4f85b83ffc4",
#         "Content-Type": "application/json"
#          }
headers={
         "token": "e8f3071444ca5cc19120a4f85b83ffc4",
        "Content-Type": "application/json"
         }

formdata={
    "pageNum": 1,
    "pageSize": 10,
    "type": 1
}
# formdata={
#     "i": key,
#     "from": "AUTO",
#     "to": "AUTO",
#     "smartresult": "dict",
#     "client": "fanyideskweb",
#     "salt": "15705176186154",
#     "sign": "e4fd4ff89c72f1851ee1da7a0fce239f",
#     "ts": "1570517618615",
#     "bv": "ca3dedaa9d15daa003dbdaaa991540d1",
#     "doctype": "json",
#     "version": "2.1",
#     "keyfrom": "fanyi.web",
#     "action": "FY_BY_CLICKBUTTION"
# }
# json.dumps(formdata, ensure_ascii=False) 将字典formdata转换为JSON
req=requests.post(url, data=json.dumps(formdata, ensure_ascii=False), headers=headers).json()
print(req)
# response=request.urlopen(req).read().decode()
#
# print(response)
# pat=r'"tgt":"(.*?)"}]]}'
# res=re.findall(pat,response)
# print(res[0])