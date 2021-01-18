from tools import db

from tools.time_convert import *
from tools.sql_mk import where_in
from tools.network import url_req
import time
import copy
from local_utils import *
from local_utils import _group_by
import json
from urllib.parse import quote


special_auther_list = ["shanghai", "zhejiang"]
auther_not_in = ", ".join(["'%s'" % special_auther for special_auther in special_auther_list])


def group_edit_error(self, group_id):
    ch_date = self.get_argument("ch_date", "")
    ch_date_end = self.get_argument("ch_date_end", "")

    check_date = self.get_argument("date", "")
    check_date_end = self.get_argument("date_end", "")
    status = self.get_argument("status", "99")
    jd_status = self.get_argument("jd_status", "99")
    auther = self.get_argument("auther", "default_auther")
    channel = self.get_argument("channel", "")
    error_level = self.get_argument("error_level", "")
    key_word = self.get_argument("key_word", "")
    source = self.get_argument("source", "")
    area = self.get_argument("area", "")
    res = get_group_edit_error(ch_date, ch_date_end, check_date, group_id, status, check_date_end, jd_status, auther, channel, error_level, key_word, source, area)
    self.write(json.dumps(res))


def get_group_edit_error(ch_date, ch_date_end, check_date, group_id, status, check_date_end, jd_status, auther, channel, error_level,
                         key_word, source, area):

    ch_st = time_str_to_ts(ch_date + " 00:00:00") if ch_date != "" else ""
    ch_et = time_str_to_ts(ch_date_end + " 00:00:00") if ch_date_end != "" else ""

    # st = today_begin() if check_date == "" else time_str_to_ts(check_date + " 00:00:00")
    # et = st + 86400 if check_date_end == "" else time_str_to_ts(check_date_end + " 00:00:00")
    st = time_str_to_ts(check_date + " 00:00:00") if check_date != "" else ""
    if check_date_end != "":
        et = time_str_to_ts(check_date_end + " 00:00:00")
    elif st != "":
        et = st + 86400
    else:
        et = ""

    source_sql = "1=1" if source == "" else "source = %s " % source
    gp_sql = "1=1" if int(group_id) == 0 else "t_edit_err_info.gp = %s " % group_id
    st_sql = '1=1' if str(status) == "99" else 'status %s' % (where_in(status.split(",")))
    jst_sql = '1=1' if int(jd_status) == 99 else 'admin_judge = %s' % jd_status
    auther_sql = '1=1' if auther == "default_auther" else "auther like '%%%s%%'" % auther
    # channel_sql = '1=1' if channel == "" else 'channel = "%s"' % (channel)
    if channel == "" and area == "":
        channel_sql = '1=1'
    else:
        resp_channel_list = js_load_req("http://172.16.198.126/manage/index/getChannelInfoByName?name={}&area={}".format(quote(channel), area)  , timeout_sec = 1 , try_count = 2)["dataList"]
        # resp_channel_list = js_load_req("http://47.96.182.117/manage/index/getChannelInfoByName?name={}&area={}".format(quote(channel), area))["dataList"]
        channel_list_temp = [item["channel"] for item in resp_channel_list]
        channel_sql = 'channel %s' % where_in(channel_list_temp) if len(channel_list_temp) > 0 else 'channel = ""'
    error_level_sql = '1=1' if error_level == "" else 'error_level = %s' % (error_level)
    kw_sql = "1=1" if key_word == "" else '`desc` like "%{}%"'.format(key_word)
    create_time_sql = " and create_time >= '%s' and create_time < '%s'" % (ts_to_time_str(st), ts_to_time_str(et)) if st != "" and et != "" else ""
    start_sql = " and start >= %s and start < %s" % (ch_st, ch_et) if ch_st != "" and ch_et != "" else ""

    where_sql = "where `desc` != '质检完成' and status != 99 and %s %s %s" % (' and '.join([source_sql, gp_sql, st_sql, jst_sql, auther_sql, channel_sql, error_level_sql, kw_sql]), create_time_sql, start_sql)

    # if st == "" and et == "":
    #     where_sql = "where  %s and `desc` != '质检完成' and status != 99" % (' and '.join([gp_sql, st_sql, jst_sql, auther_sql, channel_sql, error_level_sql, kw_sql]))
    # else:
    #     where_sql = "where  %s and create_time between '%s'  and '%s' and  `desc` != '质检完成' and status != 99" % (' and '.join([gp_sql, st_sql, jst_sql, auther_sql, channel_sql, error_level_sql, kw_sql]), ts_to_time_str(st), ts_to_time_str(et))

    select_sql = "select id, channel, end, status, err_count count, start, start check_date, auther submit_user, gp, create_time, sys_err, `desc`, error_level level, admin_judge, entry_clerk, source from t_edit_err_info %s order by start desc" % where_sql
    res = db.query(select_sql, "db_qc")

    # res_ch_man = js_load_req("http://dmp.hz-data.com/Admin/CutTask/tqc_media_permission")["mediaList"]
    # item = {}
    # for row in res_ch_man:
    #     item[str(row["fmediaid"])] = row["wx_alias"] if row["wx_alias"] is not None else ""

    res_ch_man_item = {}
    if res:
        start_1 = res[0]["start"]
        start_2 = res[-1]["start"]
        kj_ch_list = []
        for row in res:
            if row["gp"] == 90 and row["channel"] not in kj_ch_list:
                kj_ch_list.append(row["channel"])

        if kj_ch_list:
            fzr_data = {
                "mediaIdList": kj_ch_list,
                "dateList": get_day_list(ts_to_time_str(start_1, "%Y-%m-%d"), ts_to_time_str(start_2, "%Y-%m-%d"))
            }
            res_ch_man_list = js_load_req("http://d.hz-data.com/Open/CutTask/fzr", post_str=json.dumps(fzr_data) ,timeout_sec = 1 , try_count = 2)["dataList"]

            for row in res_ch_man_list:
                res_ch_man_item[row["fmediaid"]+"_"+row["fissuedate"]] = row["fzr"]

    ch_name_map = get_channel_name_map([i["channel"] for i in res])

    for i in res:
        i["channel_canme"] = ch_name_map.get(i["channel"], "??")
        i["count"] = int(i["count"])
        # i["create_time"] = i["create_time"].strftime("%Y-%m-%d %H:%M:%S")
        i["create_time"] = str(i["create_time"])
        i["check_date"] = ts_to_time_str(int(i["check_date"]), "%Y-%m-%d")
        i["sys_err"] = "yes" if i["sys_err"] == 1 else "no"
        # if i["submit_user"] == "录入退回" and i["entry_clerk"] is not None and i["entry_clerk"].strip() != "":
        #     i["submit_user"] = i["submit_user"] + "_" + i["entry_clerk"]
        if str(i["gp"]) == "90":
            i["principal"] = res_ch_man_item.get(str(i["channel"])+"_"+i["check_date"], "")
    return {"total": len(res), 'rows': res}


def edit_error_submit(self, channel_id, start, end):
    try:
        err_count = int(self.get_argument("count"))
    except:
        err_count = 1

    start ,end = int(float(start)) , int(float(end))
    desc = self.get_argument( "reason" )
    auther = self.get_argument( "submit_user" )
    error_level = self.get_argument( "level" , '0' )
    sys_err = 1 if self.get_argument( "sys_err" ,"no" ) == "yes" else 0
    entry_clerk = self.get_argument("entry_clerk", "")
    source = self.get_argument("source", '0')

    # 上海退回等同于客户退回
    if auther == 'shanghai':
        source = '2'

    if source == '0':
        if auther == "录入退回":
            auther = entry_clerk
            source = '1'
        elif auther == "客户退回":
            auther = ""
            source = '2'
        else:
            source = '0'
    
    # 上海传入基本都为一整天 从0:0:0-23:59:59 强制转为7200s
    # if auther in special_auther_list and end-start >= 83000:
    #     end = start + 72000
    # gp = db.query("select edit_lock.group gp from  edit_lock where channel = '%s' " % (channel_id), "db")[0]["gp"]
    gp = get_channel_gp(channel_id, ts_to_time_str(start, time_format="%Y-%m-%d"))

    # 插入数据前根据 channel, start, end, desc 做去重判断
    sql_select = "select id from t_edit_err_info where `channel` = '%s' and `start` = '%s' and `end` = '%s' and `desc` = '%s' and status != 99" % (
    channel_id, start, end, desc)
    res = db.query(sql_select, "db_qc")
    if res:
        sql_update = "update t_edit_err_info set err_count = '%s', auther = '%s', gp = '%s', sys_err = '%s', error_level = '%s', entry_clerk = '%s', source = '%s', create_time = '%s' where id = '%s'" % (
        err_count, auther, gp, sys_err, error_level, entry_clerk, source, ts_to_time_str(time.time()), res[-1]['id'])
        ret = db.query(sql_update, "db_qc")
    else:
        ret = db.query(
            "insert into  t_edit_err_info  (channel, start, `end`, err_count, `desc`, auther, gp, sys_err, error_level, entry_clerk, source) values('%s', %s, %s, %s, '%s', '%s', %s , %s , %s, '%s', '%s')" % (
            channel_id, start, end, err_count, desc, auther, gp, sys_err, error_level, entry_clerk, source), "db_qc")
    if desc != "质检完成" and source not in('1', '2'):
        db.query("update status_monitor set edit_flag = 7, edit_time = now(), judge_flag = -1, judge_time = now(), sync_flag = 0, sync_time = now(), dmp_flag = 0, dmp_time = now() where channel = '%s' and day = '%s'" % (channel_id, ts_to_time_str(int(start))[:10]), "db")
    if source == '1':
        # 录入退回流程走曹勇的逻辑, 过来一条更一次状态
        db.query("update status_monitor set edit_flag = 3, edit_time = now(), judge_flag = -1, judge_time = now(), sync_flag = 0, sync_time = now(), dmp_flag = 0, dmp_time = now() where channel = '%s' and day = '%s'" % (channel_id, ts_to_time_str(int(start))[:10]), "db")
    ret_write(self, ret=0, data=ret)


def edit_error_update(self, err_id, status):
    db.query("update t_edit_err_info set status = %s where id = %s" % (status, err_id), "db_qc")
    # 暂时仅质检数据支持质疑, 录入和客户退回不支持质疑
    # db.query("update t_edit_err_info set status = %s where id = %s and source = 0" % (status, err_id), "db_qc")
    # 检查完成均可以点
    # if int(status) == -1:
        # db.query("update t_edit_err_info set status = %s where id = %s" % (status, err_id), "db_qc")

    # 质疑通过, 查看该天是否完成
    if int(status) == 2:
        res1 = db.query('select channel, start from t_edit_err_info where id = %s' % err_id, "db_qc")
        channel = res1[0]['channel']
        day = ts_to_time_str(res1[0]["start"])
        res2 = db.query('select id from t_edit_err_info where channel = "%s" and left(FROM_UNIXTIME(start), 10) = "%s" and status in (0, 1, 3) and `desc` != "质检完成"' % (res1[0]['channel'], day), "db_qc")
        if not res2:
            db.query("update status_monitor set edit_flag = 5, edit_time = now(), judge_flag = -1, judge_time = now(), sync_flag = 0, sync_time = now(), dmp_flag = 0, dmp_time = now() where channel = '%s' and day = '%s'" % (channel, day), "db")

    auther = db.query("select auther from t_edit_err_info where id =  %s  " % (err_id), "db_qc")[0]["auther"]
    if -1 == int(status) and auther == 'shanghai':
        info = db.query("select channel,  start , end from t_edit_err_info where id =  %s " % (err_id), "db_qc")[0]
        ret = url_req("http://dmp.hz-data.com/Api/ShIssue/push_finish_backtask", post_str="mediaid=%s&issuedate=%s" % (
        info["channel"], ts_to_time_str((info["end"] + info["start"]) / 2, "%Y-%m-%d")) , timeout_sec = 1 , try_count = 2)
    ret_write(self, ret=0)


def edit_error_kv_update(self, err_id, key, value):
    db.query("update t_edit_err_info set %s = %s where id =   %s  " % (key, value, err_id), "db_qc")
    ret_write(self, ret=0)


def edit_error_info(self, channel_id, start, end):
    start, end = float(start), float(end)
    ret = db.query(
        "select id , start ,end  , err_count count , auther  user , `desc`  ,  create_time , status , error_level level  from   t_edit_err_info  where channel = '%s' and start between %s and %s  and auther not in (%s) and err_count > 0 and status !=  2  and status != 99" % (
        channel_id, start - 2222, end, auther_not_in), "db_qc")
    ret = list(filter(lambda x: seg_join_length(x["start"], x["end"], start, end) >= 1, ret))
    for i in ret:
        i["start"] = ts_to_time_str(i["start"])
        i["end"] = ts_to_time_str(i["end"])
        i["create_time"] = i["create_time"].strftime("%Y-%m-%d %H:%M:%S")
    ret_write(self, ret=0, data=ret)


def edit_error_rank(self, start_date, end_date):
    res = edit_error_summary(start_date, end_date)
    res = sorted(res, reverse=True, key=lambda x: x["err_perc"])
    if res[9]["err_perc"] <= 0.05:
        res = res[:10]
    else:
        res = list(filter(res, lambda x: x["err_perc"] > 0.05))
    ret_write(self, data=res)


def edit_error_work(self, checker_name, check_date):
    year, month = int(check_date[:4]), int(check_date[4:])
    if month == 12:
        year2, month2 = year + 1, 1
    else:
        year2, month2 = year, month + 1
    sql = "select * from  t_edit_err_info where auther =  '%s' and create_time between '%.4d-%.2d-01 00:00:00' and '%.4d-%.2d-01 00:00:00'  and status != 99" % (
    checker_name, year, month, year2, month2)
    res = db.query(
        "select channel , create_time , err_count from  t_edit_err_info where auther =  '%s' and create_time between '%.4d-%.2d-01 00:00:00' and '%.4d-%.2d-01 00:00:00'  and status != 99" % (
        checker_name, year, month, year2, month2), "db_qc")
    info = {}
    for i in res:
        key = i["create_time"].strftime("%Y-%m-%d")
        if key in info:
            info[key]["channel"] = info[key]["channel"] + 1
            info[key]["err_count"] = info[key]["err_count"] + i["err_count"]
        else:
            info[key] = {"channel": 1, "err_count": i["err_count"], "date": key}
    ret_write(self, data=list(info.values()))


def edit_error_summary(s_date, e_date, gp=""):
    if "" == gp:
        ch_sql = "1=1"
        err_query = "select * from  t_edit_err_info  where create_time between '%s'  and '%s' and status in ( - 1, 0 ,1, 3 )  and auther not in (%s) and %s order by channel  , start  " % (
        s_date, e_date, auther_not_in, ch_sql)
    else:
        ch_sql = "gp = %s " % gp
        err_query = "select * from  t_edit_err_info  where create_time between '%s'  and '%s' and status in ( - 1, 0 ,1, 3 )  and auther not in (%s) and %s and sys_err = 0  order by channel  , start  " % (
        s_date, e_date, auther_not_in, ch_sql)

    s_date = s_date + " 00:00:00"
    e_date = e_date + " 23:59:59"

    res = db.query(err_query, "db_qc")
    #    res = db.query( "select * from  t_edit_err_info  where create_time between '%s'  and '%s' and status in ( - 1, 0 ,1, 3 )  and auther !='shanghai' and %s and sys_err = 0  order by channel  , start  " % ( s_date, e_date , ch_sql ), "db_qc" )

    ret = []
    ch_info_map = {i["channel"]: i for i in
                   db.query("select channel , edit_lock.group gp ,channel_cname  from  edit_lock ", "db")}
    res = _group_by(res, key_func=lambda x: "%s_%s" % (x["channel"], ts_to_time_str(x["start"], "%Y-%m-%d")))
    for key, info in res.items():
        ch, data_date = key.split("_")
        if ch not in ch_info_map: continue
        last_create_time = max([x["create_time"] for x in info])
        err_sum = sum([x["err_count"] for x in info])
        day_st = time_str_to_ts(data_date + " 00:00:00")
        day_et = day_st + 86400
        summary_num = db.query(
            "select count( *) num from summary where channel = '%s' and start between %s and %s and tag in ( 0,1,3,4 ) and create_time <'%s'  " % (
            ch, day_st, day_et, last_create_time), "db")[0]["num"]
        if 0 != err_sum:
            err_perc = err_sum / (err_sum + summary_num)
        else:
            err_perc = 0.0
        ret.append({"check_date": info[0]["create_time"].strftime("%Y-%m-%d"),
                    "data_date": data_date,
                    "err_count": err_sum,
                    "summary_count": summary_num,
                    "channel": ch,
                    "gp": info[0]["gp"],
                    "sys_err": 0,
                    "channel_cname": ch_info_map[ch]["channel_cname"],
                    "err_perc": err_perc})

    ret = sorted(ret, key=lambda x: x["err_perc"], reverse=True)
    if "" == gp: return ret

    res2 = db.query(
        "select * from  t_edit_err_info  where create_time between '%s'  and '%s' and status in ( - 1, 0 ,1, 3 )  and auther not in (%s) and %s and sys_err = 1  order by channel  , start  " % (
        s_date, e_date, auther_not_in, ch_sql), "db_qc")

    filled_data = list(res.keys())

    for x in res2:
        key = "%s_%s" % (x["channel"], ts_to_time_str(x["start"], "%Y-%m-%d"))
        if key in filled_data: continue
        filled_data.append(key)
        ret.append({"check_date": x["create_time"].strftime("%Y-%m-%d"),
                    "data_date": ts_to_time_str(x["start"], "%Y-%m-%d"),
                    "err_count": x["err_count"],
                    "summary_count": 0,
                    "channel": x["channel"],
                    "sys_err": 1,
                    "gp": x["gp"],
                    "channel_cname": ch_info_map[x['channel']]["channel_cname"],
                    "err_perc": 0})
    return ret


from local_utils import _err_ansi_range


def _edit_task_exists(channel_id, start, end):
    return sum([x[1] - x[0] for x in _err_ansi_range(channel_id, start, end)]) > 3


def err_check_status(self):
    res = db.query(
        "select channel ,start ,gp ,status from t_edit_err_info where auther = 'psp' and create_time > '%s' order by status , channel " % (
            ts_to_time_str(time.time() - 86400)), "db_qc")
    ch_name_map = get_channel_name_map([i["channel"] for i in res])
    for x in res:
        x['ch_name'] = ch_name_map[x['channel']]
        x['day'] = ts_to_time_str(x['start'], '%Y-%m-%d')
        x['status'] = '完成' if x['status'] == -1 else '未完成'
        del x['start']
    self.write(json.dumps(res))


from local_utils import _group_by


def edit_error_checker(self, start_date, end_date):
    info = db.query(
        'select  channel , admin_judge ,auther , status  from t_edit_err_info  where  create_time between "%s 00:00:00" and "%s 00:00:00" and `desc` != "质检完成" and status != 99' % (
        start_date, end_date), 'db_qc')
    info2 = db.query(
        'select  channel , admin_judge ,auther , status  from t_edit_err_info  where  create_time between "%s 00:00:00" and "%s 00:00:00" and status != 99' % (
        start_date, end_date), 'db_qc')
    auther_list = set([x['auther'] for x in info])
    au_gp_auther = _group_by(info, lambda x: x['auther'])
    au_gp_auther2 = _group_by(info2, lambda x: x['auther'])
    res = {x: {'auther': x, 'total': len(au_gp_auther[x])} for x in au_gp_auther}
    for auther in res:
        ch_info2 = list(filter(lambda x: auther == x['auther'], info2))
        res[auther]['ch_count'] = len(set([x['channel'] for x in ch_info2]))  # the confirmed data also shall be marked
        ch_info = list(filter(lambda x: auther == x['auther'], info))
        res[auther]['not_error'] = len(list(filter(lambda data: 1 == data['status'], ch_info)))
        res[auther]['admin_judge_err'] = len(list(filter(lambda data: 1 == data['admin_judge'], ch_info)))
        res[auther]['effective'] = len(
            list(filter(lambda x: x['status'] in (-1, 0, 3) and 0 == x['admin_judge'], ch_info)))
        res[auther]['perc'] = '{}%'.format(int(float(res[auther]['effective']) * 100 / res[auther]['total']))

    ret = {'count': len(res), 'rows': sorted(res.values(), key=lambda x: x['auther'])}
    self.write(json.dumps(ret))


def group_sync_error(self):
    channel = self.get_argument("channel", "")
    date_start = self.get_argument("date_start", "")
    date_end = self.get_argument("date_end", "")
    channel_sql = "1=1" if channel == "" else "channel = '%s'" % channel
    date_start_sql = "1=1" if date_start == "" else "day >= '%s'" % date_start
    date_end_sql = "1=1" if date_end == "" else "day <= '%s'" % date_end
    sql = "select channel, monitor_err_info, update_time from post_process_day_job where recheck_status = 2 and %s" % " and ".join([channel_sql, date_start_sql, date_end_sql])
    res = db.query(sql, 'db')
    ch_name_map = get_channel_name_map(list(set([i["channel"] for i in res])))
    res_sync = []
    for row in res:
        dict_item = json.loads(row["monitor_err_info"])
        for key, value in dict_item.items():
            for item in value["data_list"]:
                item["admin_judge"] = 0
                item["channel_canme"] = ch_name_map.get(item["channel"], "??")
                item["count"] = 1
                item["desc"] = key
                item["sys_err"] = "yes"
                item["submit_user"] = "sync_monitor"
                item["status"] = "0"
                item["create_time"] = str(row["update_time"])
                item["gp"] = ""
                item["entry_clerk"] = ""
                res_sync.append(item)
    self.write(json.dumps({"total": len(res_sync), 'rows': res_sync}))


handler_list = [
    (r'/group_edit_error/(?P<group_id>.*)/', mk_req_handler(get=group_edit_error)),
    (r'/edit_error_summary/(?P<start>.*)/(?P<end>.*)/', mk_req_handler(get=lambda self, start, end: ret_write(self, data=edit_error_summary(start, end, self.get_argument("group", ""))))),
    (r'/edit_error_submit/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', mk_req_handler(post=edit_error_submit)),
    (r'/edit_error_update/(?P<err_id>.*)/(?P<status>.*)/', mk_req_handler(get=edit_error_update)),
    (r'/edit_error_kv_update/(?P<err_id>.*)/(?P<key>.*)/(?P<value>.*)/', mk_req_handler(get=edit_error_kv_update)),
    (r'/edit_error_info/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', mk_req_handler(get=edit_error_info)),
    (r'/edit_error_rank/(?P<start_date>.*)/(?P<end_date>.*)/', mk_req_handler(get=edit_error_rank)),
    (r'/edit_error_work/(?P<checker_name>.*)/(?P<check_date>.*)/', mk_req_handler(get=edit_error_work)),
    (r'/edit_error_checker/(?P<start_date>.*)/(?P<end_date>.*)/', mk_req_handler3(get=edit_error_checker)),
    (r'/edit_task_exists/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', mk_req_handler(get=lambda self, channel_id, start, end: ret_write(self, 0 if _edit_task_exists(channel_id, int(float(start)), int(float(end))) else 1))),
    (r'/err_check_status/', mk_req_handler(get=err_check_status)),
    (r'/group_sync_error/', mk_req_handler(get=group_sync_error))
]
