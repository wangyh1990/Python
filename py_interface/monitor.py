#coding:utf8

import os
from os.path import *

root_dir = dirname(abspath(__file__))
os.chdir(root_dir)
log_dir = join(root_dir, "logs")
if not exists(log_dir):
    os.makedirs(log_dir)

def monitor2(kw, _cmd):
    cmd = "ps -ef|grep %s|grep -v grep|awk '{print $2}'" % kw
    print(cmd)
    pid = os.popen(cmd).read().strip()
    print pid
    if len(pid) == 0:
        print(_cmd)
        os.system(_cmd)

monitor2( "ad_api_new_interface.py"  , "   source  /opt/py_envs/stream_service/bin/activate  &&  cd  /data/py_interface && nohup python ./ad_api_new_interface.py &  " )
