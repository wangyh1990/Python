from tools import db
from tools.time_convert import * 
from tools.network import * 
from tools.math import * 
from tools.decorates import * 
from local_utils import * 
import random
import uuid
import threading
from tools.utils import * 
from tools.log_factory import get_logger

def summary_get( ch , start , end , times =  -1 ):
    end = min( end , start + 86400 )
    res = db.query( "select start   , end  ,ad_uuid uuid ,score from summary where channel = '%s' and start between %s and %s and tag in ( 0,1,3 , 4 ) order by start "% ( ch , start ,end ) , "db" ) 
    for i in res : 
        if -1 != times : 
            i[ "start" ] = int( i[ "start" ] * times  )
            i[ "end" ] =  int( i[ "end" ] * times  )
    return res

def ansi_info_get_show( ch , date ):
    st = time_str_to_ts( date + " 00:00:00" )
    et = st + 86400
    task = db.query( "select start , end , status  from task where channel = '%s' and start between %s and %s  and end between %s and %s order by start   " %( ch, st - 3600 , et , st , et   ) , "db" )
    for i in range( len( task ) -1  ):
        while i + 1 <len( task ) and task[ i ][ "status" ]  ==  task[ i+ 1 ][ "status" ] and task[ i ][ "end" ] >= task[ i + 1 ][  "start"  ]:
            task[ i ][ "end" ] = task[ i + 1 ] [ "end" ]
            del task[ i + 1 ]

        if i + 1 <len( task ) and task[ i + 1 ][ "start" ] - task[ i ][  "end" ] > 0:
            task.insert( i + 1 , {  "start" : task[ i ][ "end" ] , "end" :task[ i + 1 ][ "start" ] , "status" : "-5"   } )
            task[ i + 1 ][ "duration" ] = task[ i + 1 ][ "end" ] - task[ i + 1 ][ "start" ]

    repeat = db.query( "select start , end ,status  from ad.repeat where channel = '%s' and start between %s and %s  and end between %s and %s order by start  " %( ch,  st - 3600 , et , st , et   ) , "db" )
    edit_task = db.query( "select start , end , video_status , edit_status from edit_task where channel = '%s' and start between %s and %s  and end between %s and %s order by start  " %( ch, st - 3600 ,et , st , et   ) , "db" )
    summary = db.query( "select start , end , title  from summary where channel = '%s' and start between %s and %s  and end between %s and %s and tag in ( 0,1,3 )  order by start   " %( ch ,  st - 3600 ,et , st , et   ) , "db" )
    gap_list = [ { "start": st , "end" : st } ] +  json.loads( url_req( "http://az.hz-data.xyz:8087/channel/%s/m3u8.gap?start_time=%s&end_time=%s" % ( ch ,st ,et  ) ,timeout_sec = 1 , try_count = 2 ) )[ "data" ] + [ { "start" : et , "end" : et } ]
    stream_info =  [  [ gap_list[ i ][ "end" ] ,gap_list[ i + 1 ][ "start" ] ]  for i in range(  len( gap_list   ) - 1 )   ]
    for data in [ task ,repeat , edit_task , summary ]:
        for i in data:
            i[ "start" ] = ts_to_time_str( i[ "start" ] )
            i[ "end" ] = ts_to_time_str( i[ "end" ] )
    for i in stream_info:
        for j in range( 2 ) : i[ j ] = ts_to_time_str( i[ j ] )
    return { "st" : st , "summary" : summary , "task" : task ,   "edit_task" :edit_task, "repeat" : repeat ,"stream" : stream_info }

def ansi_info_get( ch , date ):
    st = time_str_to_ts( date + " 00:00:00" )
    et = st + 86400
    task = db.query( "select start , end , status  from task where channel = '%s' and start between %s and %s  and end between %s and %s order by start   " %( ch, st - 3600 , et , st , et   ) , "db" )
    for i in range( len( task ) -1  ):
        while i + 1 <len( task ) and task[ i ][ "status" ]  ==  task[ i+ 1 ][ "status" ] and task[ i ][ "end" ] >= task[ i + 1 ][  "start"  ]:
            task[ i ][ "end" ] = task[ i + 1 ] [ "end" ]
            del task[ i + 1 ]

    repeat = db.query( "select start , end ,status  from ad.repeat where channel = '%s' and start between %s and %s  and end between %s and %s order by start  " %( ch,  st - 3600 , et , st , et   ) , "db" )
    edit_task = db.query( "select start , end , video_status , edit_status from edit_task where channel = '%s' and start between %s and %s  and end between %s and %s order by start  " %( ch, st - 3600 ,et , st , et   ) , "db" )
    summary = db.query( "select start , end , title  from summary where channel = '%s' and start between %s and %s  and end between %s and %s and tag in ( 0,1,3 )  order by start   " %( ch ,  st - 3600 ,et , st , et   ) , "db" )
    gap_list = [ { "start": st , "end" : st } ] +  json.loads( url_req( "http://az.hz-data.xyz:8087/channel/%s/m3u8.gap?start_time=%s&end_time=%s" % ( ch ,st ,et  ) ,timeout_sec = 1 , try_count = 2 ) )[ "data" ] + [ { "start" : et , "end" : et } ]
    stream_info =  [  { "start" : gap_list[ i ][ "end" ] , "end" :gap_list[ i + 1 ][ "start" ] }  for i in range(  len( gap_list   ) - 1 )   ]
    return { "st" : st , "summary" : summary , "task" : task ,   "edit_task" :edit_task, "repeat" : repeat ,"stream" : stream_info }

def channel_produce_modify( ch,  k , v  ):
    ret = db.query( "select * from edit_lock where channel = '%s'"  % ch  , "db" )
    if ret == None :
        return -1 , "数据库连接失败"
    if len( ret ) == 0:
        return -2 , "频道不存在"
    if k not in ( "channel_cname" , "ip" , "domain_name" , "enable" , "type" , "group" , "priority" ) :
        return -3 , "这个项目不能被修改"
    if k in (   "enable" , "type" , "group" , "priority") :
        try :
            set_str = "%s = %s " %( k , int( v ) )
        except :
            return -4 , "这个项目不能被修改"
    else:
        set_str = "%s = '%s' " %( k , v )
    sql = "update edit_lock set %s where channel  = '%s'" % ( set_str , ch )
    ret = db.query( sql , "db" )
    if None == ret :
        return -1 , "数据库连接失败"

    return 0 , "修改完成"

def channel_produce_status( ch_list = [] , produce_sys = "" ):
    if [] == ch_list :  
        list_sql_wh = "1=1" 
    else:
        list_sql_wh = " channel in ( %s  )" %  ",".join( '"%s"' % i  for i in ch_list )

    if produce_sys == "" :
        produce_sys_sql_wh =  "enable in ( 1 ,2 )"
    else:
        produce_sys_sql_wh =  " enable in( %s )" % { "cloud_cut"  : 1, "dmp" :2 }.get( produce_sys , 2 )

    ret = db.query( "select channel , enable from edit_lock where %s and %s and enable >=1   " % ( list_sql_wh , produce_sys_sql_wh ) , "db" )
    ret =  { i[ "channel" ]   :  {  "source" :  { 1: "cloud_cut" , 2: "dmp" , 3 :"auto" }.get( i[  "enable" ], "other" )   }  for i in ret } 

    res = db.query( "select channel , ip , domain_name , priority ,edit_lock.group gp  from edit_lock where enable >= 1 "  , "db" ) 
    for i in res : 
        ch = i[ "channel" ] 
        if ch not in ret  : continue 
        ret[ ch ][ "ip" ] =  i[ "ip" ]
        ret[ ch ][ "priority" ] =  i[ "priority" ]
        ret[ ch ][ "domain_name" ] =  i[ "domain_name" ]
        ret[ ch ][ "group" ] =  i[ "gp" ]
    return  0  ,ret

def ch_cloud_cut_status( ch ):
    ret =  db.query( "select  channel , ip , domain_name ,`group` , enable  ,type  ,priority ,channel_cname from edit_lock where channel = '%s'" % ch , "db" )
    if len( ret ) == 0 : 
        return {}
    return ret[ 0 ]

def ch_list_cloud_cut_status( ch_list  ):
    if len( ch_list ) == 0 : 
        return {}
    sql_filter = " channel in  ( %s  ) " % ",".join( [ "'%s'" % i for i in ch_list ] )
    tmp =  db.query( "select  channel , ip , domain_name ,`group` , enable  ,type  ,priority ,channel_cname from edit_lock where  %s  " % sql_filter , "db" )
    res =  { i[ "channel" ]  : i for i in tmp  }
    for i in set( ch_list ) - set( res.keys() ) : 
        res[ i ] = {}
    return res

from tools.self_iter import * 

def ch_full_status( ch , date ):
    st ,et =  time_str_to_ts(  "{} 00:00:00".format( date )  ) ,time_str_to_ts( "{} 23:59:59".format( date ) )
    err_list = []
    sql_wh =  "channel = '%s' and start between %s and %s and end between %s and %s "  % ( ch , st - 4000 , et , st ,et + 4000 )
    res = db.query( "select start , end , status  from task where %s order by start  " % sql_wh , "db" )
    if res[ 0 ][ "start" ] > st :
        err_list.append( {  "ts" : st ,"duration" :  res[ 0 ][ "start" ] - st  ,  "type"  : 0 , "info" :  {  "msg" : "task gap" }  } )

    if res[ -1 ][ "end" ] < et :
        err_list.append( {  "ts" : res[ -1 ][ "end" ] ,"duration" : et - res[ -1 ][ "end" ]  ,  "type"  : 0 , "info" :  {  "msg" : "task gap" }  } )

    for i , j in list_data( res ,2 ) :
        if j[ "start" ] - i[ "end" ] > 20:
            err_list.append( {  "ts" :  j[ "start" ] , "duration" : j[ "start"]  - i[ "end" ] , "type" : 0 , "info" :  {  "msg" : "task gap" }  } )
    for i in  res : 
        if i[ "status" ] != 2 :
            err_list.append( { "ts" : i [ "start" ] ,"duration" : i[ "end" ] - i[ "start" ] , "type" : 0 , "info" : { "msg" : "bad seg , status %s" % i[ "status" ]} } )

    res = db.query( "select start , end , status  from ad.repeat where %s  and status != 2 order by start  " % sql_wh , "db" )
    for i in res : err_list.append( { "ts" : i [ "start" ] ,"duration" : i[ "end" ] - i[ "start" ] , "type" : 1 , "info" : { "msg" : "bad repeat info  %s" % i[ "status" ]} } )

    res = db.query( "select start , end , edit_status , video_status from edit_task where %s  and not (  video_status = 2 and edit_status = 4 ) order by start  " % sql_wh , "db" )
    for i in res : err_list.append( { "ts" : i [ "start" ] ,"duration" : i[ "end" ] - i[ "start" ] , "type" : 2 , "info" : { "msg" : "bad edit_task info  %s %s" % ( i[ "edit_status" ] , i[ "video_status" ] ) } } )
    for i in err_list : 
        i[ "time_mark" ] =  ts_to_time_str( i[ "ts" ] )
        i[ "ts" ] =  int(   ( i[ "ts" ] - st ) /  60 )
    return  err_list

def produce_finished_point( ch_list ):
    ret = {}
    checked_ts = time.time() - 86400 *  10
    gp3_ch_list = [ x[ 'channel' ] for x in  db.query( "select channel from edit_lock where enable in( 1,2  ) and edit_lock.group = 3"  , 'db') ]
    for i in ch_list:
        if i in gp3_ch_list :
            ts = db.query( "select  max( end ) as ts from summary where channel =  '{}' and tag in ( 0,1 )  ".format( i ,  )  , "db" )[ 0 ][ "ts" ]
            ret[ i ] = ts
            continue
        ts = None
        ts = db.query( "select  min( end ) as ts from edit_task where channel =  '{}'  and video_status in( 0 ,1,2 ) and edit_status =  0  and start > {}  ".format( i , checked_ts )  , "db" )[ 0 ][ "ts" ]
        if None == ts: ts = db.query( "select  max( end ) as ts from edit_task where channel =  '{}'  and start > {} and edit_status in(  4 ) and duration > 30000  ".format( i  ,checked_ts)   , "db" )[ 0 ][ "ts" ]
        if None == ts: ts = db.query( "select  min( end ) as ts from ad.repeat where channel =  '{}'  and start > {} and status in( 0 ,1)  ".format( i ,checked_ts )   , "db" )[ 0 ][ "ts" ]
        if None == ts: ts = db.query( "select  max( end ) as ts from task  where channel = '{}' and start > {} and status in( 2 )   ".format( i  ,checked_ts) , "db" )[ 0 ][ "ts" ]
        ret[ i ] = ts
    return ret

from tools.sql_mk import where_in

def produce_finish_efficent( ch_list ) :
    res = db.query( "select  channel , days_before , efficient  from t_ansi_efficient  where channel %s and update_time >  '%s' order by channel ,days_before  " % ( where_in( ch_list ) , ts_to_time_str( time.time() - 4000 ) ) , "db_produce" )
    tmp = { x : [ 0 for y in range( 30 ) ] for x in ch_list}
    for i in res :
        tmp[  i[ "channel" ] ][ i[ "days_before" ] - 1  ] = i[ "efficient" ]
    res =  [{ "channel" : i , "efficient" :  tmp[ i ]  } for i in ch_list  ]
    return res

def get_undone_task_count( ch_list ) :
    res = db.query( "select  channel , count  from t_undone_edit_task_count  where channel %s and update_time > '%s' order by channel  " % ( where_in( ch_list ) , ts_to_time_str( time.time() - 4000 ) ) , "db_produce" )
    tmp = { x :  9999 for x in ch_list}
    for i in res :
        tmp[ i[ "channel" ] ] = i[ "count" ]
    res =  [{ "channel" : i , "count" :  tmp[ i ]  } for i in ch_list  ]
    return res

def get_uncheced_sp_count( ch_list ) :
    res = db.query( "select  channel , count  from  t_unchecked_sp_count  where channel %s and update_time > '%s' order by channel  " % ( where_in( ch_list ) , ts_to_time_str( time.time() - 4000 ) ) , "db_produce" )
    tmp = { x :  9999 for x in ch_list}
    for i in res :
        tmp[ i[ "channel" ] ] = i[ "count" ]
    res =  [{ "channel" : i , "count" :  tmp[ i ]  } for i in ch_list  ]
    return res

def get_tv_epg_info( ch, start , end  ):
    res = db.query( "select play_time_ts , play_date , play_end_time , title from epgs  where channel =  '%s' and play_time_ts between %s and %s order by play_time_ts limit 10 " %( ch , start - 15400  , end  )  , "tv_epg" )
    ret = []
    for i  in res :
        s1 = i[ 'play_time_ts' ]
        e1 = i[ "play_end_time" ].total_seconds() + int( i[ "play_date" ].strftime( "%s" ) ) 
        relation = seg_relation( start ,end  ,s1 , e1  )
        if relation != "cover" and  relation != "join"  : continue 
        ret.append( { "start" :s1 , "end" : e1 , "title" : i[ "title" ] }  )
    return ret 

def get_recog_result( sp_uuid ) :
    res = db.query( "select url , id from audio_task where name  = '%s'" % ( sp_uuid ), "audio_to_word" ) 
    if len( res ) ==  0 : 
        return {}
    res2 = db.query( "select start, end  , asr_content  word  from sub_audiotask where task_id   = %s and asr_content != ''  order by start " % ( res[ 0 ][ 'id' ] ), "audio_to_word" ) 
    for i in res2 : 
        i[ "start" ] = i[ "start" ] / 1000.0
        i[ "end" ] = i[ "end" ] / 1000.0
    return { "url" : res[ 0 ][ "url" ] , "word" : res2  }

def checked_edit_task_insert( channel ,res_start ,res_end  ,title  ,editor_id):
    edit_uuid = str( uuid.uuid1() )
    start = res_start - 10
    end = res_end - 10
    edit_result = '{"uuid": "00000000-0000-0000-0000-000000000001", "nickname": "%s", "wx_id": "11011", "tip_clips": [], "data": [{"start": %s , "end": %s , "adname": "%s"}], "channel": "%s"}'%( editor_id ,  res_start ,res_end , title ,  channel )
    sql = "insert into edit_task ( task_uuid ,  edit_uuid, channel, start, end, start_time, end_time, duration , create_time , video_status , edit_status , edit_result  , video_path )\
                            values ( '__view_created_done_edit_task__' ,   '%s', '%s', '%.3f', '%.3f',  \
                            from_unixtime('%s'), from_unixtime('%s'), '%.3f', now() , 2 , 2 ,'%s' ,  '')" % (
                                    edit_uuid, channel, start, end
                                    , start, end, end-start , edit_result) 
    db.query( sql )

def add_sample_info_to_ad_list( sample_data ):
    #how tag should be 
    ch ,st ,et , tag  ,editor_id , title  = sample_data[ "channel" ] , time_str_to_ts(sample_data[ "start" ] )   , time_str_to_ts( sample_data[ "end" ] )  , sample_data[ "tag" ] or 0  , sample_data[ "editor_id" ]  , sample_data[ "name" ]

    ret = db.query( "select count( * ) num  from ad_list  where channel =  '%s'  and floor( start )= %s and floor( end ) = %s  "  %( ch , st ,et ) , "db_bad_sample"  )

    if len( ret ) != 0 and ret[ 0 ][ "num" ] != 0 : 
        return -1 , "重复插入数据，请更换时间"

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


    if et - st > 300 :  # for  long_ad
        ad_uuid = uuid.uuid4()
        summary_insert( ch , st , et , editor_id, title , ad_uuid , 1 , 100  ,by_force = True )  
        status = 0 
        sql = "insert into sample (channel, start, end, start_time, end_time, title, uuid, duration, tag, status, create_time, editor_id) values ('%s', '%.3f','%.3f',from_unixtime('%s'),from_unixtime('%s'), '%s', '%s', '%.3f', '%s', '%s', now(), '%s')" % (ch, st, et, st, et, title, ad_uuid, et -  st , 3, status, editor_id)
        db.query( sql )
        return 0  , "成功"

    if et - st < 3:
        return -4 , "编辑时长过短"

    st =  st  + random.uniform(0.001, 0.101 ) 
    et =  et  + random.uniform(0.001, 0.101 ) 

    ret = summary_insert( ch , st , et , editor_id, title , "" , 4 , 100 ) 
    if not ret :
        return -2 , "播放记录插入失败"

    checked_edit_task_insert( ch ,st ,et  ,title  ,editor_id)
    return 0  , "成功"

def bad_summary_list( ch , start , end ): 
    return db.query( "select start, end , title  , ad_uuid , score , tag ,editor_id   from summary  where channel = '%s' and start between %s and %s and end between %s and %s  and tag not in ( 0,1,3,4 )  order by start  limit 500  " % ( ch ,start - 5400 , end , start , end + 5400 ) )

def get_tip_data( ch , start , end ): 
    return [ { "start" : 0 , "end" : 111 , "title" : "这真的是一个广告" } ]

