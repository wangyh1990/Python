#!/usr/bin/python3
import requests
import json
# url="http://dmp.hz-data.com/Admin/Ad/mergeAdUpdateRelated"
url = "http://47.96.182.117/index/getUuidUrl?channel=11000000004696&uuid=5a13d4fe-ecc7-11ea-951d-00163e084d98"
r = requests.get(url)
print(r.status_code)
# print(r.content)
content_json = json.loads(r.content)
print(json.dumps(content_json, sort_keys=True, indent=4, separators=(',', ':')))
print(json.dumps(content_json))
print("url:", content_json['url'])
