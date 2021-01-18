from local_utils import *
from tools.network import *
from tools.log_factory import get_logger
from tools import db
from tools.sql_mk import where_in
from tools.math import *
from tools.utils import *
from local_utils import _err_ansi_range
import json
import copy
import threading
import pymysql
import datetime

#@fc_test( "11000010002383" , '2019-12-12' )
def _channel_sample_rec( channel , start_date  ):
    end_date = str(datetime.date.today())
    res = db.query("select id, start_date from status_monitor_rec where channel = '%s'" % channel, "db")
    if res:
        if time_str_to_ts(str(res[0]["start_date"]), "%Y-%m-%d") < time_str_to_ts(start_date, "%Y-%m-%d"):
            start_date = str(res[0]["start_date"])
        db.query("update status_monitor_rec set start_date = '%s', end_date = '%s', status = 0 where id = %s" % (start_date, end_date, res[0]["id"]), "db")
    else:
        db.query("insert into status_monitor_rec(channel, start_date, end_date, create_time) values('%s', '%s', '%s', now())" % (channel, start_date, end_date), "db")

def get_edit_task(self, group_id):
    #    res  =  { "channel" :"11000010003174" , "start" : 1558670400, "end"   : 1558684799 , "edit_uuid" :  "uuiduuiduuid" , "channel_cname" : "java" }
    #    ret_write( self, 0 , res )
    #    return

    sql_in = """
 in( '11000010008609' , 
         '11000010008610' , 
         '11000010008611' , 
         '11000010008612' ) 
    """

    res = db.query(
        "select  channel , start ,end , edit_uuid  from edit_task2 where channel %s   and status in ( 0 ,1  ) and  channel in ( select channel from edit_lock where status = 0  and edit_lock.group = %s )  order by start limit 1  " % (
            sql_in, group_id))
    # res = db.query( "select  channel , start ,end , edit_uuid  from edit_task2 where channel in ( select channel from edit_lock where status = 0  and edit_lock.group = %s ) and status in ( 0 ,1  )  order by start limit 1  " %( group_id ) )
    # res = db.query( "select  channel , start ,end , edit_uuid  from edit_task2 where channel in ( select channel from edit_lock where status = 0  and edit_lock.group = %s ) and status in ( 0 ,1  )  order by start limit 1  " %( group_id ) )
    if len(res) == 0:
        ret_write(self, -1, {})
        return
    res = res[0]
    res["channel_cname"] = get_channel_name_map([res["channel"]]).get(res["channel"], '')
    db.query("update edit_task2 set status =  1  where edit_uuid = '%s'" % (res["edit_uuid"]))
    db.query("update edit_lock set status =  1  where channel = '%s'" % (res["channel"]))
    ret_write(self, 0, res)


def edit_task_update(self, edit_uuid, channel, editor):
    db.query("update edit_task2 set status =  2 ,editor = '%s'  where edit_uuid = '%s'" % (editor, edit_uuid))
    db.query("update edit_lock set status =  0  where channel = '%s'" % (channel))
    ret_write(self, 0)


@try_catch([])
def get_epg_info(ch, start, end):
    ch_type = db.query("select type from edit_lock where channel = '%s' " % (ch))[0]["type"]
    if ch_type == 0: res = get_tv_epg_info(ch, start, end)
    res = db.query(
        "select title , start , end from programs  where channel_vid in (  select vid from channels where hzid = '%s' ) and start between %s and %s and end between %s and %s limit 10 " % (
            ch, start - 15400, end, start, end + 15400), "bro_epg")
    for i in res:
        i["end"] = min(i["end"], end)
        i["start"] = max(i["start"], start)
    return res


@try_catch([])
def get_summary_info(ch, start, end):
    return db.query(
        "select start, end , title  , ad_uuid ,score ,tag   from summary  where channel = '%s' and start between %s and %s and end between %s and %s  limit 500  " % (
            ch, start - 5400, end, start, end + 5400))
    # return db.query( "select start, end , title  , ad_uuid ,score  from summary  where channel = '%s' and start between %s and %s and end between %s and %s and tag in ( 0 , 1 ,3 ,4 )  limit 500  " % ( ch ,start - 5400 , end , start , end + 5400 ) )


INF = 0xfffffffffff


def ch_gap_info(channel_id, start_time, end_time):
    ch_collect_info = db.query(
        "select max( start ) _max_  ,  min( start ) _min_   from t_local_stream_ref_%s " % channel_id, "ts_stream_bk")
    if None == ch_collect_info or len(ch_collect_info) == 0 or ch_collect_info[0]["_max_"] == None:
        return [{"start": start_time, "end": end_time}]

    ch_collect_start = ch_collect_info[0]["_min_"]
    ch_collect_end = ch_collect_info[0]["_max_"]

    if 0 == ch_collect_start:
        return [{"start": start_time, "end": end_time}]
    ret = db.query(
        "select start, end from t_err_stream_seg  where channel = '%s'  and status = 0 and( start between %d and %d ) group by start order by start " % (
            channel_id, start_time - 86400, end_time + 86400), "ts_stream")
    err_info = list(ret)

    err_info = [{"start": 0, "end": ch_collect_start}] + err_info + [{"start": ch_collect_end, "end": INF}]
    err_info = cp_and_join(lambda x, y: x["end"] >= y["start"], lambda x, y: {"start": x["start"], "end": y["end"]},
                           err_info)

    res = []
    for i in err_info:
        tmp = seg_join(i["start"], i["end"], start_time, end_time)
        if len(tmp) == 0 or tmp[1] - tmp[0] < 4: continue
        res.append({"start": tmp[0], "end": tmp[1]})
    return res


from tools.math import *


@try_catch([])
def get_exception_info(channel_id, start, end):
    ret = js_load_req(
        "http://172.16.198.126/index/getCrosstalkInfo?channel=%s&start=%s&end=%s" % (channel_id, start, end))
    if ret["code"] != 0 or len(ret["dataList"]) == 0:
        res = []
    else:
        res = [{"start": x["e_start"], "end": x["e_end"]} for x in ret["dataList"]]

    for x in set([ts_to_time_str(y, "%Y-%m-%d") for y in list(range(start, end, 86400)) + [end]]):
        ret = js_load_req(
            "http://172.16.198.126/manage/channel/getChannelServiceTime?channel=%s&date=%s" % (channel_id, x))
        if ret["code"] != 0 or len(ret["dataList"]) == 0:
            continue
        res = res + [{"start": y["start"], "end": y["end"]} for y in ret["dataList"]]
    res = range_list_combine(sorted(res, key=lambda x: x['start']), "start", "end")

    while len(res) != 0 and res[0]["end"] < start:
        del res[0]

    while len(res) != 0 and res[-1]["start"] > end:
        del res[-1]

    if res == []: return []
    res[0]["start"] = max(res[0]["start"], start)
    res[-1]["end"] = min(res[-1]["end"], end)
    return res


#    return res

def _get_view_data(channel_id, start, end):
    start, end = int(start), int(end)
    epg_info = dict_list_update(get_epg_info(channel_id, start, end), {"type": 2})
    tmp_summary_info = get_summary_info(channel_id, start, end)
    formal_summary_data = [i for i in tmp_summary_info if i["tag"] in (0, 1, 3, 4)]
    [i.pop("tag") for i in formal_summary_data]
    summary_info = dict_list_update(formal_summary_data, {"type": 0})
    tmp_info = sorted(ch_gap_info(channel_id, start, end) + get_exception_info(channel_id, start, end),
                      key=lambda x: x["start"])
    gap_info = dict_list_update(tmp_info, {"type": 1})

    res = sorted([{"start": start, "end": start, "type": -10}, {"start": end, "end": end, "type": -10}] + list(
        summary_info) + gap_info, key=lambda x: x["start"])

    res = res + [{"start": i["end"], "end": j["start"], "type": -1} for i, j in list_data(res, 2) if
                 i["end"] < j["start"] - 3]

    for i in [x["start"] for x in tmp_summary_info if "tag" in x]:
        for j in res:
            if i >= j["start"] and i <= j["end"]:
                j["tag"] = 1
                break
    for i in res:
        if i["type"] == -1:
            i["tip"] = 1
            break

    return sorted(list(filter(lambda x: x["type"] != - 10, res)) + list(epg_info),
                  key=lambda x: x["start"] * 100 - x["type"])


def get_view_data(self, channel_id, start, end):
    res = _get_view_data(channel_id, start, end)
    self.write(json.dumps({"ret": 0, "channel_id": channel_id, "data": res}))


def get_view_data2(self, channel_id, start, end):
    start, end = int(start), int(end)
    res = _get_view_data(channel_id, start, end)
    tmp = [{"range_start": x, "range_end": min(x + 3600 * 4, end), "data": []} for x in range(start, end, 4 * 3600)]
    for x in res:
        ds, de = x["start"], x["end"]
        for y in tmp:
            rs, re = y["range_start"], y["range_end"]
            if rs <= ds and de <= re:
                y["data"].append(x)
            elif rs <= ds and ds <= re and de >= re:
                p = copy.copy(x)
                p["end"] = re
                y["data"].append(p)
            elif ds <= rs and rs <= de and de <= re:
                p = copy.copy(x)
                p["start"] = rs
                y["data"].append(p)

    self.write(json.dumps({"ret": 0, "channel_id": channel_id, "data": tmp}))


def checked_edit_task_insert(channel, res_start, res_end, title, editor_id):
    edit_uuid = str(uuid.uuid1())
    start = res_start - 10
    end = res_end - 10
    edit_result = '{"uuid": "00000000-0000-0000-0000-000000000001", "nickname": "%s", "wx_id": "11011", "tip_clips": [], "data": [{"start": %s , "end": %s , "adname": "%s"}], "channel": "%s"}' % (
        editor_id, res_start, res_end, title, channel)
    sql = "insert into edit_task ( task_uuid ,  edit_uuid, channel, start, end, start_time, end_time, duration , create_time , video_status , edit_status , edit_result  , video_path )\
                            values ( '__view_created_done_edit_task__' ,   '%s', '%s', '%.3f', '%.3f',  \
                            from_unixtime('%s'), from_unixtime('%s'), '%.3f', now() , 2 , 2 ,'%s' ,  '')" % (
        edit_uuid, channel, start, end
        , start, end, end - start, edit_result)
    db.query(sql)


def add_sample_info_to_ad_list(sample_data):
    # how tag should be
    ch, st, et, tag, editor_id, title = sample_data["channel"], time_str_to_ts(sample_data["start"]), time_str_to_ts(
        sample_data["end"]), sample_data["tag"] or 0, sample_data["editor_id"], sample_data["name"]

    ret = db.query(
        "select count( * ) num  from ad_list  where channel =  '%s'  and floor( start )= %s and floor( end ) = %s  " % (
            ch, st, et), "db_bad_sample")

    if len(ret) != 0 and ret[0]["num"] != 0:
        return -1, "重复插入数据，请更换时间"

    # test later  no need to do it here and now
    #    if sample_data[ "sample_path" ]
    #        tag = 2
    #        db.query( "insert into ad_list ( channel , start , end , name , editor_id  , uuid ,sample_path , tag  )  values ( '%s' , %s , %s , '%s' , '%s' , '%s' , %s ) "  %( ch ,
    #                  st ,
    #                  et ,
    #                  title ,
    #                  editor_id,
    #                  sample_data [ "uuid" ] ,
    #                  sample_data [ "sample_path" ] ,
    #                  tag) ,
    #                  "db_bad_sample"  )
    #        return 0  , "成功"

    if et - st > 300:  # for  long_ad
        ad_uuid = uuid.uuid4()
        summary_insert(ch, st, et, editor_id, title, ad_uuid, 1, 100, by_force=True)
        status = 0
        sql = "insert into sample (channel, start, end, start_time, end_time, title, uuid, duration, tag, status, create_time, editor_id) values ('%s', '%.3f','%.3f',from_unixtime('%s'),from_unixtime('%s'), '%s', '%s', '%.3f', '%s', '%s', now(), '%s')" % (
            ch, st, et, st, et, title, ad_uuid, et - st, 3, status, editor_id)
        db.query(sql)
        return 0, "成功"

    if et - st < 3:
        return -4, "编辑时长过短"

    st = st + random.uniform(0.001, 0.101)
    et = et + random.uniform(0.001, 0.101)

    ret = summary_insert(ch, st, et, editor_id, title, "", 4, 100)
    if not ret:
        return -2, "播放记录插入失败"

    checked_edit_task_insert(ch, st, et, title, editor_id)
    return 0, "成功"


def sample_info_add(self):
    try:
        para_list = ["channel", "start", "end", "name", "uuid", "editor_id", "tag", "sample_path", "show", "show_start",
                     "show_end"]
        para = {i: None for i in para_list}
        for i in para.keys():
            para[i] = self.get_argument(i, None)
            if para[i] == None and i in ("star", "end", "channel"):
                self.write(json.dumps({"code": 1, "msg": "失败参数不够"}))
                return
        ret, msg = add_sample_info_to_ad_list(para)
        try:
            if 0 == ret and para["show"]:
                self.write(json.dumps({"code": ret, "msg": msg,
                                       "view_data": _get_view_data(para["channel"], int(para["show_start"]),
                                                                   int(para["show_end"]))}))
            else:
                self.write(json.dumps({"code": ret, "msg": msg}))
        except Exception as err:
            pass
            get_logger().error(" %s is error" % err)
    except Exception as err:
        get_logger().error(" %s is error" % err)


def summary_seg_join(s1, t1, s2, t2):
    if s1 >= t1 or s2 >= t2 or t1 <= s2 or t2 <= s1: return False
    d = min(t1, t2) - max(s1, s2) + 0.0
    return d > 1.0 or d / (t1 - s1) > 0.2 or d / (t2 - s2) > 0.2


def summary_seg_join_long(s1, t1, s2, t2):
    d =seg_join_length( s1,t1,s2,t2 ) 
    return d > 0

def summary_cross(ch, start, end):
    if start >= end: return False
    if end - start >= 300: return summary_cross_check_long(ch, start, end)
    data_range = 1800
    sql_cmd = "select start ,end from summary_%s where ( start between %d and %d )  and ( end  between %d  and %d   ) and tag in ( 0 , 1 , 4  ) and duration < 300  " % (
        ch, start - data_range, end + data_range, start - data_range, end + data_range)
    res_list = db.query(sql_cmd, "db_fast")
    for i in res_list:
        if summary_seg_join(start, end, i["start"], i["end"]): return True

    return False 

def summary_cross_check_long(ch, start, end):
    data_range = 3800
    sql_cmd = "select start ,end from summary where channel = '%s' and ( start between %d and %d )  and ( end  between %d  and %d   ) and tag in ( 0 ,1 , 4  ) and duration >= 300  " % (
        ch, start - data_range, end + data_range, start - data_range, end + data_range)
    res_list = db.query(sql_cmd, "db")
    for i in res_list:
        if summary_seg_join_long(start, end, i["start"], i["end"]):
            return True
    return False

def ad_list_cross(ch, start, end, name):
    res = db.query(
        "select count( * ) num from ad_list where channel = '%s' and start between %s and %s and end between %s and %s  and name = '%s'  and  create_time > '%s' " % (
            ch, start - 0.3, start + 0.3, end - 0.3, end + 0.3, name, ts_to_time_str(time.time() - 60)),
        "db_bad_sample")
    return res[0]["num"] > 0



def time_str_to_ts_cor( time_str ):
    if '.' not in time_str :
        return time_str_to_ts( time_str )
    tmp = time_str.split( "." )
    return   time_str_to_ts( tmp[ 0 ] ) +  float( '0.' +  tmp[ 1 ] ) 

def sample_info_add2(self):
    try:
#        print( self )
        ch = self.get_argument("channel")
        editor_id = self.get_argument("editor_id", "")
        name = self.get_argument("name")

        try :
            start = time_str_to_ts_cor (  self.get_argument("start") )
            end = time_str_to_ts_cor (  self.get_argument( 'end' ) )
        except :
            start = float(self.get_argument("start"))
            end = float(self.get_argument("end"))

        business_type = self.get_argument("business_type", '-1')
        ad_type = self.get_argument("ad_type", '-1')
        if business_type == '': business_type = -1
        if ad_type == '': ad_type = -1
        ad_brand = self.get_argument("ad_brand", '')
        ad_owner = self.get_argument("ad_owner", '')
        ad_in_show = self.get_argument("ad_in_show", '0') == '0'  # 1 for yes ,0 for no
        ad_speaker = self.get_argument("ad_speaker", '')
        ver_info = self.get_argument("ver_info", '')

        info = {"business_type": business_type,
                "ad_brand": ad_brand,
                "ad_owner": ad_owner,
                "ad_in_show ": ad_in_show,
                "ad_speaker ": ad_speaker,
                "ver_info": ver_info,
                "ad_type": ad_type}
        # business_type           商业类别 int
        # ad_type                 广告类别  int
        # ad_brand                广告品牌 string
        # ad_owner                广告主 string
        # ad_in_show              栏目广告 bool
        # ad_speaker              代言人  string
        # ver_info                版本说明string
        if end <= start:
            self.write(json.dumps({"code": 3, "msg": "开始时间大于结束时间"}))
            return

        if end - start <= 2.98 :
            self.write(json.dumps({"code": 4, "msg": "广告长度小于三秒　请关联播放记录"}))
            return
        if end -  start < 3.0 :
            end += 0.03
        ret = _sample_info_add2(ch, start, end, name, editor_id, add_info=info)
        ret = {"code": ret[0], "msg": ret[1] , 'edit_uuid' : ret[ 2 ] }
        self.write(json.dumps(ret))
    except Exception as err:
        self.write(json.dumps({"code": 1, "msg": "参数有误"}))


def ad_insert(ch, start, end, name, editor_id, add_info):
    db.query(
        "insert into ad_list ( channel , start ,end ,name , editor_id ,tag  ) values( '%s' , %s , %s ,  '%s' , '%s'  , 1 )" % (
            ch, start, end, name, editor_id), "db_bad_sample")
    ins_fc = short_ad_ins if end - start < 300 else long_ad_ins
    return ins_fc(ch, start, end, name, editor_id, info=add_info)


#@fc_test( '11000010003435' , 0  , 1 ,  'java' , 'BB'  )
def short_ad_ins(channel, start, end, title, editor_id, info={}, tag=0):
    _channel_sample_rec( channel , ts_to_time_str(  start , "%Y-%m-%d" ) )
    edit_uuid = str(uuid.uuid1())
    edit_result = {"uuid": "00000000-0000-0000-0000-000000000001",
                   "nickname": editor_id,
                   "wx_id": "11011",
                   "tip_clips": [],
                   "data": [{"start": start, "end": end, "adname": "%s" % title, 'info': info}],
                   "channel": channel}
    if tag != 0: edit_result['data '][0]['tag'] = tag
    sql = "insert into edit_task ( task_uuid ,  edit_uuid, channel, start, end, start_time, end_time, duration , create_time , video_status , edit_status , edit_result )\
                            values ( '__view_created_done_edit_task__' ,   '%s', '%s', '%.3f', '%.3f',  \
                            from_unixtime('%s'), from_unixtime('%s'), '%.3f', now() , 2 , 2 ,'%s' )" % (
        edit_uuid, channel, start, end
        , start, end, end - start, json.dumps(edit_result, ensure_ascii=False))
    db.query(sql, 'db_fast')

    ad_uuid = edit_uuid
    sql = "insert into summary_%s (edit_uuid, start, start_time, end, end_time, duration, title, editor_id, ad_uuid, create_time , score ,tag ,day  ) values ('%s','%s',from_unixtime('%s'),'%s',from_unixtime('%s'),'%s','%s','%s', '%s', now() , 100 ,4 , '%s'  )" % (
            channel, edit_uuid,  start, start, end, end, end - start, title, editor_id, ad_uuid ,ts_to_time_str( start , '%Y-%m-%d' ))
    db.query(sql, 'db_fast')
    return True , edit_uuid


def long_ad_ins(channel, start, end, title, editor_id, info):
    return False , ''
    duration = end - start
    ad_uuid, edit_uuid = str(uuid.uuid1()), str(uuid.uuid1())
    status = 4
    sql = "insert into sample (channel, start, end, start_time, end_time, title, uuid, duration, tag, status, create_time, editor_id ,  info) values ('%s', '%.3f','%.3f',from_unixtime('%s'),from_unixtime('%s'), '%s', '%s', '%.3f', '%s', '%s', now(), '%s' , '%s')" % (
        channel, start, end, start, end, title, ad_uuid, duration, 3, status, editor_id, json.dumps(info))
    db.query(sql)
    sql = "insert into summary (edit_uuid, channel, start, start_time, end, end_time, duration, title, editor_id, ad_uuid, create_time , score ,tag) values ('%s','%s','%s',from_unixtime('%s'),'%s',from_unixtime('%s'),'%s','%s','%s', '%s', now() , 100 , 4 )" % (
        edit_uuid, channel, start, start, end, end, duration, title, editor_id, ad_uuid)
    db.query(sql)
    return True ,edit_uuid 


##@double_call_forbid((120, "同时间点重复提交，请等待"))
#@fc_test( '11000010002382' ,  1578440905 , 1578440997  , '公益（时代楷模黄大发）', 'BB' )
def _sample_info_add2(ch, start, end, name, editor_id, add_info={}):
    if end <= start:
        return 5, "开始节点小于结束节点" ,''
    try:
        if summary_cross(ch, start, end):
            return 2, "播放记录交叉，请调整时间点, 如果确认无误可以试试刷页面 " , ''
        ret = ad_insert(ch, start, end, name, editor_id, add_info)
        if ret[ 0 ] :
            url_req('http://192.168.0.153:10023/audio_detect', post_str= 'channel=%s&s_time=%s&e_time=%s&id=%s' %( ch , start, end ,ret[ 1 ]  ) )
#            url_req('http://118.31.55.177:10023/audio_detect', post_str= 'channel=%s&s_time=%s&e_time=%s&id=%s' %( ch , start, end ,ret[ 1 ]  ) )
            return 0 , "录入成功" , ret[ 1 ]
        else:
            return -2, '录入失败请联系技术人员' ,''
    except Exception as err:
        return -1, '%s' % err , ''


def sample_repush(self, channel_id, ad_uuid):
    db.query("update sample set tag = 4  where channel = '%s'  and uuid = '%s' " % (channel_id, ad_uuid))
    ret_write(self, 0)


def edit_task_disable(self, channel_id, check_date):
    db.query(
        "update edit_task set edit_status = 4 where  channel  ='%s' and start_time between '%s 00:00:00' and '%s 23:59:59'  and edit_status = 0 " % (
            channel_id, check_date, check_date))
    db.query("update produce_info_count set count = 0  where  channel  ='%s' and start_time = '%s'  and type = 2  " % (
        channel_id, check_date), "db_produce_info")

    start, end = time_str_to_ts("%s 00:00:00" % check_date), time_str_to_ts("%s 23:59:59" % check_date)
    sql = "insert into edit_task ( task_uuid  , edit_uuid, channel, start, end, start_time, end_time, duration , video_status , edit_status  )\
                            values ( '__long_ad_job__mark__' , '%s', '%s', '%.3f', '%.3f', from_unixtime('%s'), from_unixtime('%s'), '%.3f',  2 , 4  )" % (
        str(uuid.uuid1()), channel_id, start, end, start, end, end - start)
    db.query(sql)
    kw_max("%s_etd" % (channel_id), end)
    ret_write(self, 0)


def ad_sample_remake(self):
    ch = self.get_argument("ch")
    sp_uuid = self.get_argument("sp_uuid")
    start_date = self.get_argument("check_date")
    remove_summary_info = db.query(
        "select start , end , title from summary where channel = '%s' and ad_uuid = '%s' and start_time >='%s' and tag in( 0, 1 )  order by start  " % (
            ch, sp_uuid, start_date), "db")
    
    def foo():
        [summary_delete_notify(ch, x["start"]) for x in remove_summary_info]
        tmp = remove_summary_info[0]
        if tmp["end"] - tmp["start"] < 300:
            _sample_info_add2(ch, tmp["start"], tmp["end"], tmp["title"], "dmp_reset")
            return
        for x in remove_summary_info:
            _sample_info_add2(ch, x["start"], x["end"], x["title"], "dmp_reset")

    threading.Thread(target=foo).start()
    db.query("update summary set tag = 18 where channel = '%s' and ad_uuid = '%s' and start_time >='%s' " % (
        ch, sp_uuid, start_date), "db")
    ret_write(self, 0)


def edit_task_update(self, channel, start, end):
    return


def err_ansi_range(self, channel_id, start, end):
    ret_write(self, 0, data=_err_ansi_range(channel_id, start, end))


def _sp_name_tip(channel_id, start, end, wd):
    try:
        res = db.query(
            "select title ,uuid  , duration from sample where channel = '{}' and  tag in (  1,2,3 ) and  title like '%{}%' ".format(
                channel_id, wd))
        res = sorted(res, key=lambda x: abs(x["duration"] - (end - start)))
        return res[: 10]
    except Exception as err:
        get_logger().error(" %s is error" % err)
        return []


class Tmp(CrossDomainHandler3):
    def prepare(self):
        self.set_header('Access-Control-Allow-Credentials', 'true')
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('X-Content-Type-Options', 'nosniff')

    #        self.set_header('Content-Type', 'text/html')

    def get(self, channel_id, start, end):
        try:
            wd = self.get_argument("key")
            start, end = float(start), float(end)
            ret_write(self, 0, data=_sp_name_tip(channel_id, start, end, wd))
        except Exception as err:
            ret_write(self, -1, [])


# 新增广告done
# 删除样片
# 删除播放记录
# 样片置过期接口
# 传入了token

import redis
import json
from tools.decorates import *

G_redis_pool = redis.ConnectionPool(host='118.31.55.177', password='10b8cf715cd9340b35f7d4a483bc497b', port=6379,
                                    decode_responses=True)


@try_catch(None)
def get_token_info(token):
    r = redis.Redis(connection_pool=G_redis_pool)
    info = r.get(token)
    return None if info is None else json.loads(info)


def sample_info_add3(self):
    token = self.get_argument("token", None)
    if not token:
        self.write(json.dumps({"code": -1, "msg": "token not exista"}))
        return
    info = get_token_info(token)
    if not info:
        self.write(json.dumps({"code": -1, "msg": "bad token"}))
        return
    ch = self.get_argument("channel")
    if 1 == info["admin"] or ch in info["ch_list"]:
        sample_info_add2(self)
    else:
        self.write(json.dumps({"code": -1, "msg": "权限不够"}))

def summary_delete(self, channel_id, idx ):
    who = self.get_argument( "who" ,'' )
    if '' ==  who :
        ret_write(self, -1, '谁删除的呢？ ')
        return 
    db.query( 'update summary_%s set tag =  16 ,edit_uuid = "%s" where  id = %s ' % (  channel_id,who , idx ) , 'db_fast' )
    ret_write(self, 0, '成功删除')


from tools.utils import summary_delete_notify

def update_summary(tmp):
    url = "http://dmp.goldclippers.com/Open/Ai/add_issue?person_code=100101&person_password=e10adc3949ba59abbe56e057f20f883e"
    js_load_req(url, post_str=json.dumps(tmp), try_count=111)


def foo2(ch, day, sp_info):
    res = js_load_req("http://dmp.goldclippers.com/Open/Ai/get_media_issue", post_str="mediaId=%s&date=%s" % (ch, day))[
        "issueList"]
    res = sorted(res, key=lambda x: x["start"])
    tmp = list(filter(lambda x: x['count'] > 1, group_by(res, lambda x: x["start"])))
    if len(tmp) > 0:
        [summary_delete_notify(ch, x["data"]["start"]) for x in tmp]
        res = \
            js_load_req("http://dmp.goldclippers.com/Open/Ai/get_media_issue",
                        post_str="mediaId=%s&date=%s" % (ch, day))[
                "issueList"]
        res = sorted(res, key=lambda x: x["start"])

    dmp_start_set = set([int(x['start']) for x in res])

    start, end = time_str_to_ts(day + ' 00:00:00'), time_str_to_ts(day + ' 23:59:59')

    res2 = db.query(
        "select start , end ,ad_uuid uuid ,title ,update_time ,score   from summary  where channel = '%s' and start between %s and %s and tag in( 0 , 1 ,3 ) and title != '本媒体宣传' and title != '台标'  and title  !=  '节目预告' order by start" % (
            ch, start, end))
    res2 = list(filter(lambda x: seg_join_length(x["start"], x["end"], start, end) > 0.1, res2))

    tag = False
    for x, y in list_data(res2, 2):
        if "remove" in y: continue
        if int(x["start"]) != int(y['start']): continue
        if x["end"] > y["end"]:
            db.query(
                "update summary  set tag = 14  where channel = '%s' and start = '%s' and ad_uuid = '%s' limit 1 " % (
                    ch, y["start"], y["uuid"]))
            y["remove"] = 1
        if x["end"] < y["end"]:
            db.query(
                "update summary  set tag = 14  where channel = '%s' and start = '%s' and ad_uuid = '%s' limit 1 " % (
                    ch, x["start"], x["uuid"]))
            x["remove"] = 1

    res2 = list(filter(lambda x: "remove" not in x, res2))
    edit_start_set = set([int(x["start"]) for x in res2])
    remote_update = []
    for x in res2:
        if int(x["start"]) in dmp_start_set: continue
        join_len = -1
        x["old_start"] = x["start"]
        x["old_end"] = x["end"]
        for y in res:
            y["start"] = int(y["start"])
            y["end"] = int(y["end"])
            com_len = seg_join_length(x["start"], x["end"], int(y["start"]), int(y["end"]))
            join_len = max(join_len, com_len)
            if com_len < 0.001: continue

            if x["start"] < y["start"] and y["start"] < x["end"]:
                x["end"] = y["start"]
            elif x["start"] < y["end"] and y["end"] < x["end"]:
                x["start"] = y["end"]
        if join_len / (x["end"] - x['start']) > 0.1:
            continue
        if x["start"] != x["old_start"] or x["end"] != x["old_end"]:
            db.query(
                "update summary set update_time = update_time , start = %s , end = %s where channel = '%s' and  start = %s and end = %s  and ad_uuid = '%s' limit 1  " % (
                    x["start"], x["end"], ch, x["old_start"], x["old_end"], x["uuid"]))
        del x["update_time"]
        del x["old_start"]
        del x["old_end"]
        remote_update.append(x)
    #        break
    if len(remote_update) > 0:
        update_summary({"mediaId": ch, "issueList": remote_update})
    return

def foo3(ch, day):
    res = js_load_req("http://dmp.goldclippers.com/Open/Ai/get_media_issue", post_str="mediaId=%s&date=%s" % (ch, day))[
        "issueList"]
    res = sorted(res, key=lambda x: x["start"])
    tmp = list(filter(lambda x: x['count'] > 1, group_by(res, lambda x: x["start"])))
    if len(tmp) > 0:
        [summary_delete_notify(ch, x["data"]["start"]) for x in tmp]
        res = \
            js_load_req("http://dmp.goldclippers.com/Open/Ai/get_media_issue",
                        post_str="mediaId=%s&date=%s" % (ch, day))[
                "issueList"]
        res = sorted(res, key=lambda x: x["start"])
    dmp_start_set = set([int(x['start']) for x in res])

    start, end = time_str_to_ts(day + ' 00:00:00'), time_str_to_ts(day + ' 23:59:59')

    res2 = db.query(
        "select start , end ,ad_uuid uuid ,title ,update_time ,score   from summary  where channel = '%s' and start between %s and %s and tag in( 0 , 1 ,3 ) and title != '本媒体宣传' and title != '台标'  and title  !=  '节目预告' order by start" % (
            ch, start, end))
    res2 = list(filter(lambda x: seg_join_length(x["start"], x["end"], start, end) > 0.1, res2))

    for x in res2:
        if int(x["start"]) in dmp_start_set: continue
        db.query('update summary set tag  = 16 where channel = "%s" and start = %s and tag in ( 0,1,3 ) limit 1 ' % (
            ch, x["start"]))
    return

def audio_detect_state_by_channel(self):
    ch = self.get_argument( 'channel' )
    day = self.get_argument( 'date' )
    return 
    self.write( url_req( 'http://192.168.0.153:10023/audio_detect_state_by_channel?channel=%s&date=%s' %( ch ,day  ) ) )

#@fc_test( 'f1ba9440-1ae4-4fd0-af83-233826ac36a7' , '老人鞋' )
def sp_check_update_sample_summary(sp_uuid, title):
    db.query( "update sample set `tag` = 1, title = '%s' where uuid = '%s' and tag =  4  " % ( title , sp_uuid ) )
    db.query( "update summary set `tag` = 0, title = '%s' where ad_uuid = '%s' and tag = 4  " % ( title , sp_uuid ) ) 

def sp_check(self):
    channel_id = self.get_argument("channel_id")
    sp_uuid = self.get_argument("sp_uuid")
    st = self.get_argument("status")
    msg = self.get_argument("msg", "")
    additional_info = self.get_argument("detail", '{}')
    if additional_info is None or additional_info.strip() == "":
        additional_info = "{}"
    title = json.loads(additional_info).get('fadname', "")
    additional_info = pymysql.escape_string(additional_info)

    res = db.query("select id from sample_confirm where channel = '%s' and sample_uuid = '%s' limit 1" % (channel_id, sp_uuid))
    if res:
        sql = "update sample_confirm set status = '%s', msg = '%s', additional_info = '%s' where id = '%s'" % (st, msg, additional_info, res[0]['id'])
    else:
        sql = "insert into sample_confirm(channel, sample_uuid, status, msg, additional_info, create_time) values('%s','%s','%s','%s','%s','%s')" % (
                channel_id, sp_uuid, int(st), msg, additional_info, ts_to_time_str(time.time()))
    db.query(sql, "db")

    if int(st) == 1:
        sp_check_update_sample_summary( sp_uuid, title )

    if int(st) == -1:
        sql = "insert into `uuid` (uuid,fid,source,error_cause,error_id) values ('%s','%s', 0, '%s', '%s')" % (sp_uuid, channel_id, msg, msg.split(";;")[0].strip())
        db.query(sql, "db_produce_info")
    ret_write(self, 0)

def update_sync_flag(self):
    channel = self.get_argument("channel", "")
    day = self.get_argument("day", "")
    ret = 1
    res = js_load_req("http://172.16.198.126/index/rewriteChannelStatus", post_str="channel=%s&date=%s" % (channel, day))
    if int(res["code"]) == 0:
        db.query("update post_process_day_job set status = -10 where channel = '%s' and `day` = '%s'" % (channel, day))
        ret = 0
    ret_write(self, ret)


def sync_monitor(self):
    channel = self.get_argument("channel", "")
    day = self.get_argument("day", "")
    type = self.get_argument("type", "simple")
    where_channel = "channel = '%s'" % channel if channel != "" else "1 = 1"
    where_day = "day = '%s'" % day if day != "" else "1 = 1"

    sql = "select idx, channel, `day`, recheck_status, monitor_err_info, update_time from sync_day_job where status != -10 and %s" % " and ".join([where_channel, where_day])
    res = db.query(sql, 'db')

    def _sync_monitor_simple(res):
        if res and res[0]["recheck_status"] == 1:
            data = {"code": 0, "msg": "完成"}
        else:
            data = {"code": 1, "msg": "未完成"}
        return data

    def _sync_monitor_detail(res):
        data = {}
        data["list"] = []
        for row in res:
            item = {"code": 1, "msg": "未完成"}
            if row["recheck_status"] == 1:
                item["msg"] = "完成"
                item["code"] = 0
            elif type == "detail" and row["recheck_status"] == 2:
                item["monitor_err_info"] = json.loads(row["monitor_err_info"])
            elif type == "detail" and row["recheck_status"] == 0:
                item["msg"] = "未开始同步"
            item["recheck_status"] = row["recheck_status"]
            item["channel"] = row["channel"]
            item["day"] = str(row["day"])
            item["update_time"] = str(row["update_time"])
            data["list"].append(item)
        data["total_count"] = len(data["list"])
        return data

    data = _sync_monitor_detail(res) if type == "detail" else _sync_monitor_simple(res)
    ret_write(self, 0, data)


def _uncheckek_tasks( channel_id , atype):
    def _for_check(channel_id):
        res = db.query( "select edit_result , edit_status ,create_time    from edit_task where channel = '{}' and edit_status in ( 2,3,5,6 ) and edit_result like '%start%'  ".format( channel_id), 'db')
        info = []
        for x in res:
            ad_info = json.loads(x['edit_result'])
            info.append({'editor_id': ad_info['nickname'], 'title': ad_info['data'][0]['adname'],
                         'status': '未处理' if x['edit_result'] == 2 else '处理中',
                         'past_time': time.time() - time_str_to_ts(x['create_time'].strftime("%Y-%m-%d %H:%M:%S"))})
        return info

    def _for_edit(channel_id):
        res = db.query(
            "select edit_result , edit_status ,create_time    from edit_task where channel = '{}' and edit_status in ( 2,3   ) and edit_result like '%start%'  ".format(
                channel_id), 'db')
        info = []
        for x in res:
            ad_info = json.loads(x['edit_result'])
            info.append({'editor_id': ad_info['nickname'], 'title': ad_info['data'][0]['adname'],
                         'status': '未处理' if x['edit_result'] == 2 else '处理中',
                         'past_time': time.time() - time_str_to_ts(x['create_time'].strftime("%Y-%m-%d %H:%M:%S"))})
        return info
    info = _for_check(channel_id) if 'check' ==  atype else _for_edit(channel_id)
                                                      #nothing
    return info

def uncheckek_tasks(self, channel_id):
    info = _uncheckek_tasks( channel_id , self.get_argument("type", 'nothing') ) 
    self.write(json.dumps({'total': len(info), 'rows': info}))

def get_day_list(start, end):
    datestart = datetime.datetime.strptime(start, '%Y-%m-%d')
    dateend = datetime.datetime.strptime(end, '%Y-%m-%d')
    day_list = [str(datestart)[:10]]
    while datestart < dateend:
        datestart += datetime.timedelta(days=1)
        datestart.strftime('%Y-%m-%d')
        day_list.append(str(datestart)[:10])
    return day_list


def get_adjacent_day(interval):
    today = datetime.date.today()
    interval_day = datetime.timedelta(days=abs(interval))
    if interval >= 0:
        adjacent_day = today + interval_day
    else:
        adjacent_day = today - interval_day
    return str(adjacent_day)


def channel_task_confirm(self):
    channel = self.get_argument("channel")
    day = self.get_argument("day")
    confirm_time = self.get_argument("confirm_time")
    dict_data = _uncheckek_tasks( channel, 'check' )
    if len(dict_data) == 0:
        day_list = get_day_list(day, get_adjacent_day(-1))
        res_1 = db.query("select id, `day`, rate from task_confirm where channel = '%s'" % channel, "db")
        for row in res_1:
            if str(row["day"]) in day_list and row["rate"] >= 0.9:
                js_load_req("http://172.16.198.126/index/saveTaskStatus", post_str="channel=%s&date=%s&status=%s" % (channel, row["day"], 1))
    else:
        db.query("insert into channel_task_lock(channel, `day`, confirm_time, create_time) values('%s','%s','%s', now())" % (
        channel, day, confirm_time), "db")
    ret_write(self, 0)

#@fc_test( "" , '11000000014696'  , '2019-12-04' )
def info_sync_ready( self  ,channel_id , day ):
    res = db.query( "select  1 from post_process_day_job   where channel =  '%s' and recheck_status = 1 and day  = '%s'" %( channel_id , day  ) )
    ret_write(self, 0 if len( res ) == 1 else - 1 )

def channel_day_task_check(self):
    channel = self.get_argument("channel")
    day = self.get_argument("day")
    ret = 1
    res = db.query("select rate from task_confirm where channel = '%s' and `day` = '%s'" % (channel, day), "db")
    if res and res[0]["rate"] >= 0.9:
        dict_data = _uncheckek_tasks( channel, 'check' )
        if len(dict_data) == 0:
            ret = 0
    ret_write(self, ret)


def rec_monitor(self):
    channel = self.get_argument("channel", "")
    where_sql = "1=1" if channel == "" else "channel = '%s'" % channel
    sql = "select * from rec_monitor where %s" % where_sql
    res = db.query(sql, 'db')
    data = {}
    for row in res:
        for key in row.keys():
            if "_time" in key:
                row[key] = str(row[key])
        view_url = "http://47.96.182.117/view/check/taskEdit2.html?id={}&date={}&group=".format(row["channel"], row["start_time"][:10])
        if row["channel"] in data.keys() and row["start_time"][:10] in data[row["channel"]].keys():
            data[row["channel"]][row["start_time"][:10]]["error_info"].append(row)
        elif row["channel"] in data.keys():
            data[row["channel"]][row["start_time"][:10]] = {"error_info": [row], "view_url": view_url}
        else:
            data[row["channel"]] = {row["start_time"][:10]: {"error_info": [row], "view_url": view_url}}
    ret_write(self, 0, data)


def get_status_item(pname, row):
    if pname == "flow":
        msg_add = ",断流比:{}%,断流次数:{}".format(row["du_sc"], row["error_duration"])
        msg = "流正常" if row[pname + "_flag"] == 1 else "流异常"
        msg = msg + msg_add
    elif pname == "rec":
        msg = "识别完成" if row[pname + "_flag"] == 1 else "识别未完成"
    elif pname == "edit":
        if row[pname + "_flag"] == 0:
            msg = "该频道天无需编辑确认"
        elif row[pname + "_flag"] == 1:
            msg = "待编辑确认"
        elif row[pname + "_flag"] == 2:
            msg = "编辑已确认"
        elif row[pname + "_flag"] == 3:
            msg = "录入退回, 待编辑再次确认"
        elif row[pname + "_flag"] == 4:
            msg = "该频道天有新增或删除的播放记录, 待编辑确认"
        elif row[pname + "_flag"] == 5:
            msg = "编辑再次确认完成"
        elif row[pname + "_flag"] == 6:
            msg = "客户退回, 待编辑再次确认"
        elif row[pname + "_flag"] == 7:
            msg = "质检退回, 待编辑再次确认"
        else:
            msg = "编辑状态还未更新"
    elif pname == "judge":
        if row[pname + "_flag"] == 3:
            msg = "录入完成"
        elif row[pname + "_flag"] == 2:
            msg = "录入已完成, 但有样片错误编辑还未处理"
        elif row[pname + "_flag"] == 1:
            msg = "录入未完成, 且已有样片错误"
        else:
            msg = "录入未完成"
    elif pname == "sync":
        if row[pname + "_flag"] == 2:
            msg = "同步有异常"
        elif row[pname + "_flag"] == 1:
            msg = "同步完成"
        else:
            msg = "同步未完成"
    elif pname == "client_judge":
        if row[pname + "_flag"] == 1:
            msg = "客户退回处理中"
        elif row[pname + "_flag"] == 2:
            msg = "客户退回再次同步完成"
        else:
            msg = "无客户退回,状态正常"

    item = {
        "flag": row[pname + "_flag"],
        "msg": msg,
        "update_time": str(row[pname + "_time"])
    }
    if pname == "edit":
        item["edit_confirm_time"] = str(row["edit_confirm_time"])
    elif pname == "flow":
        item["du_sc"] = row["du_sc"]
        item["error_duration"] = row["error_duration"]
    elif pname == "rec":
        item["detail_url"] = "http://47.96.182.117/manage/channel/getChannelServiceTime?channel={}&date={}".format(row["channel"], row["day"])
    return item


def status_monitor(self):
    channel = self.get_argument("channel", "")
    day = self.get_argument("day", "")
    res = db.query("select * from status_monitor where channel = '%s' and `day` = '%s'" % (channel, day), 'db')
    if res:
        data = {}
        for pname in ["flow", "rec", "edit", "judge", "sync", "client_judge"]:
            data[pname + "_status"] = get_status_item(pname, res[0])
    else:
        data = {}
    ret_write(self, 0, data)


def change_flow_flag(self):
    channel = self.get_argument("channel", "")
    day = self.get_argument("day", "")
    res = db.query("select du_sc, error_duration from gather_error_sc where channel = '%s' and batch_date = '%s'" % (channel, day), "db_produce_info")
    if float(res[0]['du_sc']) >= 40 or int(res[0]['error_duration']) >= 200:
        flow_flag = 0
    else:
        flow_flag = 1
    res_1 = db.query("select id from status_monitor where channel = '%s' and `day` = '%s'" % (channel, day), "db")
    if res_1:
        db.query("update status_monitor set flow_flag = %s, flow_time = now() where id = %s" % (flow_flag, res_1[0]["id"]), "db")
    else:
        db.query("insert into status_monitor(channel, `day`, flow_flag, flow_time, create_time) values('%s', '%s', %s, now(), now())" % (channel, day, flow_flag), "db")
    ret_write(self, 0)


def channel_sample_rec(self):
    channel = self.get_argument("channel", "")
    start_date = self.get_argument("day", "")
    _channel_sample_rec( channel , start_date  )
    ret_write(self, 0)


def edit_confirm(self):
    channel = self.get_argument("channel")
    day = self.get_argument("day")
    edit_confirm_time = self.get_argument("confirm_time")
    res = db.query("select edit_flag from status_monitor where channel = '%s' and `day` = '%s'" % (channel, day), "db")
    if res and res[0]["edit_flag"] in (3, 6):
        # 编辑再次确认完成
        edit_flag_new = 5
    else:
        edit_flag_new = 2
    db.query("update status_monitor set edit_flag = %s, edit_confirm_time = '%s', edit_time = now(), judge_flag = 0, judge_time = now(), sync_flag = 0, sync_time = now(), dmp_flag = 0, dmp_time = now() where channel = '%s' and `day` = '%s'" % (edit_flag_new, edit_confirm_time, channel, day), "db")
    ret_write(self, 0)


def get_err_before_edit(self):
    # post_data_temp = self.request.body_arguments
    # post_data = {"channel_list": []}
    # for key in post_data_temp.keys():
    #     if key == "channel_list":
    #         for channel in post_data_temp[key]:
    #             post_data["channel_list"].append(channel.decode("utf-8"))
    #     else:
    #         post_data[key] = post_data_temp.get(key)[0].decode("utf-8")
    channel_list_str = self.get_argument("channel_list", "")
    channel_list = channel_list_str.split(",")
    start_date = self.get_argument("start_date", "")
    end_date = self.get_argument("end_date", "")

    # res = db.query("select channel, `day`, edit_flag from status_monitor where channel in (%s) and `day` >= '%s' and `day` <= '%s' and (flow_flag = 0 or rec_flag != 1 or edit_flag = 3)" % (",".join(["'%s'" % channel for channel in post_data["channel_list"]]), post_data["start_date"], post_data["end_date"]), "db")
    res = db.query("select channel, `day`, edit_flag from status_monitor where channel in (%s) and `day` >= '%s' and `day` <= '%s'" % (",".join(["'%s'" % channel for channel in channel_list]), start_date, end_date), "db")

    data = {}
    for row in res:
        # if row["edit_flag"] in (2, 5):
        #     continue
        if row["channel"] not in data.keys():
            data[row["channel"]] = {}

        if row["edit_flag"] in (3, 7):
            # 3录入退回 7质检退回
            data[row["channel"]][str(row["day"])] = 3
        elif row["edit_flag"] == 4:
            # 编辑再次确认后影响编辑的天
            data[row["channel"]][str(row["day"])] = 5
        elif row["edit_flag"] == 6:
            # 客户退回
            data[row["channel"]][str(row["day"])] = 6
        # else:
        #     # 流异常或识别异常, 不需要给到前台了
        #     data[row["channel"]][str(row["day"])] = 2
    ret_write(self, 0, data)


def judge_back_day(self):
    channel = self.get_argument("channel", "")
    day = self.get_argument("day", "")
    status = self.get_argument("status", 0)
    reason = self.get_argument("reason", "")
    start = time_str_to_ts(day + " 00:00:00")
    end = time_str_to_ts(day + " 23:59:59")
    res = db.query("select enable from edit_lock where channel = '%s'" % channel, "db")
    if res and str(res[0]["enable"]) == "1" and int(status) == -1:
        gp = db.query("select edit_lock.group gp from edit_lock where channel = '%s' " % channel, "db")[0]["gp"]
        db.query("insert into t_edit_err_info (channel, start, `end`, err_count, `desc`, auther, gp, sys_err, error_level, entry_clerk) values('%s', %s, %s, %s, '%s', '%s', %s , %s , %s, '%s')" % (channel, start, end, 1, reason, "客户退回", gp, 0, 0, ""), "db_qc")
        db.query("update status_monitor set edit_flag = 6, edit_time = now(), judge_flag = 0, judge_time = now(), client_judge_flag = 1, client_judge_time = now(), sync_flag = 0, sync_time = now(), dmp_flag = 0, dmp_time = now() where channel = '%s' and `day` = '%s'" % (channel, day), "db")
        ret_write(self, 0)
    else:
        ret_write(self, 1)


def summary_cross_1( ch , new_start , new_end ,  self_idx  ):
    if new_start >= new_end :
        return True
    sql_cmd = "select start , end from summary_%s where  start between %s and %s  and tag in ( 0 , 1 , 3, 4  ) and id != %s "%  ( ch , new_start - 3233  ,new_end +  3333 , self_idx )
    res_list = db.query( sql_cmd , "db_fast" ) 
#    print( res_list )
    if len(  res_list ) == 0 :
        return False
    for x in res_list :
        if summary_seg_join( new_start,new_end , x[ 'start' ]  ,x[ 'end' ])  : 
            return True
    return False

#@fc_test( '11000010002382', 1579104727 , 1579104736 , '6659' )
def _summary_alter( channel ,new_start ,new_end ,  self_idx ):
    if summary_cross_1( channel , new_start ,new_end   , self_idx ):
        return -1 , '您所修改的位置和其他的播放重复了'
    sql_form  = '''
        update summary_%s set start = %s , end = %s , start_time = '%s' , end_time ='%s' ,duration = %s  , score = 105 where  id =  %s
    '''
    db.query( sql_form %(  channel ,  new_start , new_end , ts_to_time_str( new_start ), ts_to_time_str( new_end )  ,new_end - new_start  , self_idx  ) ,'db_fast' )  
    return 0 , 'done' 

@para_check([ 'new_start' ,'new_end' ]  )
def summary_alter( self , channel_id , idx ):
    s2,e2 = float( self.get_argument( "new_start" ) ) , float( self.get_argument( "new_end" ) )
    if e2 - s2 < 3.0 :
        self.write( json.dumps( { 'ret' : -1 , 'msg': '修改后长度小于3s'  } ) )
        return 
    ret , msg =  _summary_alter( channel_id ,  s2 , e2 ,idx )
    self.write( json.dumps( { 'ret' : ret , 'msg': msg  } ) )

def sp_modify( ch, sp_uuid  ,auther ,action ):
    try :
        sp_auther =  db.query( "select editor_id  from sample where channel = '%s' and uuid = '%s' " %( ch  , sp_uuid ) , 'db' )[ 0 ][ 'editor_id' ]
#            sample_delete_notify( ch ,sp_uuid )
    except Exception as err :
        sp_auther = ''
        pass
    if 'timeout' ==  action :
        error_cause = 'view页面置过期'
    elif 'remove' ==  action :
        error_cause = 'view页置删除'
    sp_new_tag = 7
    db.query( "update sample set tag = %s where channel = '%s' and uuid = '%s' limit 1  " %( sp_new_tag ,  ch  , sp_uuid ) , 'db_fast' )
    para = ( sp_uuid , ch , auther , sp_auther , error_cause  )
    db.query( "insert into uuid (   uuid , fid , auther , sp_auther, error_cause ,error_id , source , status )  values ( '%s' , '%s' , '%s' , '%s' , '%s' , -1 , 1 , 1 ) " % para , 'db_bad_sample' )


def sp_time_out( self , channel_id , sp_uuid , ts ):
    ts = float( ts )
    who =  self.get_argument( "auther"  ,'') 
    if  ''  == who:
        ret_write( self , -1 ,'谁删除的呢?' )
        return
    sp_modify( channel_id , sp_uuid , who , 'timeout' )
    db.query( "update summary_%s set  tag = 8 where ad_uuid ='%s' and start >   %s - 1  " %( channel_id ,sp_uuid ,  float( ts ) - 2 ) , 'db_fast' ) 
    ret_write( self , 0 ,'ok' )

#@fc_test( '',  '11000010002382' , '2020-01-17' )
def day_job_done( self , channel_id , day ):
    db.query( 'update ch_day_status set status  = 1 where channel = "%s" and day >= "%s"  ' % ( channel_id , day )  , 'db_fast' )
    ret_write( self , 0 )


def list_to_dict( list_data , hash_fc ):
    return { hash_fc( x ) :x for x in list_data  }

#@fc_test( '',  '11000010003744' , '2020-02-11' )
def combine_mp3( self , channel_id , day ):
    res =  db.query( 'select url , mp4_md5 , day ,ind  , media_type  from combine_mp3 where channel  =  "%s" and day = "%s" and status = 0 and url like "http%%" '  %( channel_id , day)  , 'db_fast' ) 
    res2 = list_to_dict( res , lambda x :"%s_%s" % (  x[ 'ind' ] , x[ 'media_type' ] )  )
    data = []
    for x in range( 1, 4  ) :
        tmp_dic = {}
        tmp_dic['url' ] = res2.get( '%s_0' % x ,{} ).get( 'url', '' )
        tmp_dic['json' ] = tmp_dic['url' ].replace( '.mp3'  ,'.json' )
        tmp_dic['mp4' ] = res2.get( '%s_1' %x  ,{} ).get( 'url'  , '' )
        tmp_dic['mp4_md5' ] = res2.get( '%s_1' %x  ,{} ).get( 'mp4_md5'  , '' )
        data.append( tmp_dic )

    ret_write( self , 0 ,  data )

#@fc_test( '',  '11000010002825' , '2020-02-11' )
def combine_mp3_v1( self , channel_id , day ):
    res =  db.query( 'select ind ,  url from combine_mp3_v1 where channel  =  "%s" and day = "%s" and media_type= 0 and status = 0 and url like "http%%"   '  %( channel_id , day)  , 'db_fast' ) 
    res2 = list_to_dict( res , lambda x : int(  x[ 'ind' ])   )
    data = []
    for x in range( 1 , 7  ) :
        tmp_dic = {}
        tmp_dic['url' ] = res2.get(  x , {} ).get( 'url' , '' )
        tmp_dic['json' ] = tmp_dic['url' ].replace( '.mp3'  ,'.json' )
        data.append( tmp_dic )
    ret_write( self , 0 ,  data )

#@fc_test( '',  '11200000004192' , '0' , '9999999999' )
def combine_mp3_reset( self , channel_id , start , end ):
    day_start ,day_end = ts_to_time_str( float( start ) ,  '%Y-%m-%d' ) ,  ts_to_time_str( float( end ) ,  '%Y-%m-%d' )
    db.query( "update combine_mp3 set status = 1 where channel = '%s' and day >= '%s' and day<= '%s' " %( channel_id  ,day_start , day_end)  , 'db_fast' ) 
    db.query( "update combine_mp3_v1 set status = 1 where channel = '%s' and day >= '%s' and day<= '%s' " %( channel_id  ,day_start , day_end)  , 'db_fast' ) 
    ret_write( self , 0  )

handler_list = [
#    (r'/edit_task/(?P<group_id>.*)/', mk_req_handler(get=get_edit_task)),
#    (r'/edit_task_update/(?P<edit_uuid>.*)/(?P<channel>.*)/(?P<editor>.*)/', mk_req_handler(get=edit_task_update)),
#    (r'/edit_task_disable/(?P<channel_id>.*)/(?P<check_date>.*)/', mk_req_handler(get=edit_task_disable)),
#    (r'/view_data/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', mk_req_handler(get=get_view_data)),
#    (r'/view_data2/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', mk_req_handler(get=get_view_data2)),
#    (r'/sample_repush/(?P<channel_id>.*)/(?P<ad_uuid>.*)/', mk_req_handler(get=sample_repush)),
#    (r'/add_sample', mk_req_handler(post=sample_info_add)),
    (r'/fast/add_view_result', mk_req_handler(post=sample_info_add2)),
#    (r'/add_new_result', mk_req_handler(post=sample_info_add2)),
#    (r'/sp_time_out_and_restore', mk_req_handler(get=ad_sample_remake)),
#    (r'/err_ansi_range/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', mk_req_handler(get=err_ansi_range)),
#    (r'/sp_name_tip/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', Tmp),
#    (r'/ad_info_add', mk_req_handler(post=sample_info_add3)),
#    (r'/ad_info_remove', mk_req_handler(post=sample_info_add2)),
#    (r'/ad_mark_remove', mk_req_handler(post=sample_info_add2)),
#    (r'/ad_info_timeout', mk_req_handler(post=sample_info_add2)),
    (r'/fast/summary_delete/(?P<channel_id>.*)/(?P<idx>.*)/', mk_req_handler(get=summary_delete)),
    (r'/fast/summary_alter/(?P<channel_id>.*)/(?P<idx>.*)/', mk_req_handler( get = summary_alter  ) ) ,
    (r'/fast/day_job_done/(?P<channel_id>.*)/(?P<day>.*)/', mk_req_handler( get = day_job_done ) ) ,
    (r'/fast/sp_time_out/(?P<channel_id>.*)/(?P<sp_uuid>.*)/(?P<ts>.*)/', mk_req_handler( post = sp_time_out  ) ) ,
    (r'/fast/combine_mp3/(?P<channel_id>.*)/(?P<day>.*)/', mk_req_handler( get = combine_mp3  ) ) ,
    (r'/fast/combine_mp3_v1/(?P<channel_id>.*)/(?P<day>.*)/', mk_req_handler( get = combine_mp3_v1  ) ) ,
    (r'/fast/combine_mp3_reset/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', mk_req_handler( get = combine_mp3_reset  ) ) ,
    (r'/audio_detect_state_by_channel', mk_req_handler(get=audio_detect_state_by_channel)),
#    (r'/summary_sync/(?P<channel_id>.*)/(?P<day>.*)/', mk_req_handler(get=summary_sync)),
#    (r'/sync_monitor/', mk_req_handler(get=sync_monitor)),
#    (r'/sp_check', mk_req_handler(post=sp_check)),
#    (r'/uncheckek_tasks/(?P<channel_id>.*)/', mk_req_handler(get=uncheckek_tasks)),
#    (r'/task_confirm', mk_req_handler(post=channel_task_confirm)),
#    (r'/info_sync_ready/(?P<channel_id>.*)/(?P<day>.*)/',  mk_req_handler( get = info_sync_ready ) ) ,
#    (r'/channel_day_task_check/', mk_req_handler(get=channel_day_task_check)),
#    (r'/rec_monitor/', mk_req_handler(get=rec_monitor)),
#    (r'/update_sync_flag', mk_req_handler(post=update_sync_flag)),
#    (r'/status_monitor/', mk_req_handler(get=status_monitor)),
#    (r'/change_flow_flag', mk_req_handler(post=change_flow_flag)),
#    (r'/channel_sample_rec', mk_req_handler(post=channel_sample_rec)),
#    (r'/edit_confirm', mk_req_handler(post=edit_confirm)),
#    (r'/judge_back_day', mk_req_handler(post=judge_back_day)),
#    (r'/get_err_before_edit', mk_req_handler(post=get_err_before_edit)),
#    #(r'/sp_check/(?P<channel_id>.*)/(?P<sp_uuid>.*)/',    mk_req_handler( post = sp_check ) )
#    #(r'/common_summary_exists/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', mk_req_handler( get = common_summary_exists ) ) ,
]

#url_req( 'http://192.168.0.153:10023/audio_detect_state_by_channel?channel=%s&date=%s' %( ch ,day  ) )

#url_req('http://118.31.55.177:10023/audio_detect', post_str= 'channel=%s&s_time=%s&e_time=%s&id=%s' %( '11000010002362' , 1577833104, 1577833119,  'dedab1d6-5180-11ea-9761-00163e084d98' ) )


