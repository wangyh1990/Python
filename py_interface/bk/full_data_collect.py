from tools import db
from tools.time_convert import * 
from tools.network import * 
from tools.decorates import * 
from tools.math import * 
from produce import * 

import traceback 
import os,sys

def is_main( i ):
    if i == 1 :
        return "[来源]"
    return ""

def stream_info_to_str( stream_type ,  info ):
    return stream_type + "," +  ", ".join([  "%s:%s %s" %( i[ "source" ] , i[ "channel_outer_id" ] , is_main( i[ "is_main" ] )  )   for  i in info.values() ]  )


def get_tv_bro_list():
    bro_list =  [ i[ "channel" ] for i in  json.loads( url_req( "http://47.96.182.117/index/getChannelInfoById?type=2" ,timeout_sec = 1 , try_count = 2 ) )[ "dataList" ] ]
    tv_list =  [ i[ "channel" ] for i in json.loads( url_req( "http://47.96.182.117/index/getChannelInfoById?type=1"  ,timeout_sec = 1 , try_count = 2) )[ "dataList" ] ]
    return tv_list , bro_list

def res_switch( info ):
    a =  {  0 : "N" , 1 : "Y" , 2 : "? " ,  }
    info[ "is_box" ] = a[ info[ "is_box" ] ]
    info[ "is_qingting" ] = a[ info[ "is_qingting" ] ]
    info[ "is_web" ] = a[ info[ "is_web" ] ]
    info[ "is_match" ] = a[ info[ "is_match" ] ]
    if info[ "producing" ] :
        info[ "producing" ] = "Y"
    else:
        info[ "producing" ] = "N"

def ch_detail_rep_get():
    channel_producing =  _producting_ch_list()
    all_ch_list = []
    tmp =  json.loads( url_req( "http://47.96.182.117/index/getAllChannel?is_use=0"  ,timeout_sec = 1 , try_count = 2) )[ "dataList" ]
    for i in tmp : 
        i[ "confirmed" ] = "Y"
        i[ "outer_collected" ] = "N"

    all_ch_list =all_ch_list  +  tmp

    tmp =  json.loads( url_req( "http://47.96.182.117/index/getAllChannel?is_use=1" ,timeout_sec = 1 , try_count = 2 ) )[ "dataList" ]
    for i in tmp : 
        i[ "confirmed" ] = "Y"
        i[ "outer_collected" ] = "Y"

    all_ch_list =all_ch_list  +  tmp

    tmp =  json.loads( url_req( "http://47.96.182.117/index/getAllChannel?is_use=2" , timeout_sec = 1 , try_count = 2 ) )[ "dataList" ]
    for i in tmp : 
        i[ "confirmed" ] = "N"
        i[ "outer_collected" ] = "Y"
            

    all_ch_list =all_ch_list  +  tmp
    res = ""

    title_list = ( "频道id",
                   "频道名称",
                   "频道类型",
                   "已确认存在（ by zhh ） ",
                   "是否采集 ",
                   "匹配到云盒",
                   "匹配到互联网采集 ",
                   "匹配到qt",
                   "是否生产 ", 
                   "源头比对结果" ,
                   "主流来源" ,
                   )

        
    res = res +  ",".join( title_list )  + "\n"

    bro_collecting_list =  [ i[ "channel" ] for i in db.query( "select channel from edit_lock where enable = 1" , "db2" )  ]
    box_collecting_list  = [ i[ "channel" ] for i in json.loads( url_req( "http://47.96.182.117/index/getBoxChannelList?is_main=2"  , timeout_sec = 1 , try_count = 2 ) )[ "dataList" ] ]

    tv_list,bro_list  = get_tv_bro_list()
    stream_info =  collecing_stream_info() [ "data" ][ "collect" ]
    for i in all_ch_list:
        ch = i[ "channel" ]
        if  ch in stream_info  :
            i[ "stream_info" ] = stream_info_to_str( stream_info[ ch ][ "main_stream_type" ] , stream_info[ ch ][ "stream_info" ]) 
            channel_source_name =  stream_info[ ch ][ "main_stream_type" ]

            if ch in bro_collecting_list and ch in box_collecting_list :
                i[ "collect_advice" ] = "erro_both"
            elif (  ch in box_collecting_list and channel_source_name == "云盒" ) or (  ch in bro_collecting_list and channel_source_name == "internet bro producing" ) :
                i[ "collect_advice" ] = "well_match"
            else:
                if  ch in box_collecting_list :
                    i[ "collect_advice" ] = "not_matched | now is %s | should be cloud box" % ( channel_source_name   ) 
                elif  ch in bro_collecting_list : 
                    i[ "collect_advice" ] = "not_matched | now is %s | should be internet bro" % ( channel_source_name  ) 
                else :
                    i[ "collect_advice" ] = "not_matched | now is %s | nowhere in " % ( channel_source_name  ) 
        else:
            i[ "stream_info" ] = "没有"
            i[ "collect_advice" ] = "empty"

        if ch in tv_list and ch in bro_list:
            i[ "ch_type" ] = "err_both"
        elif ch in tv_list:
            i[ "ch_type" ] = "tv"
        elif ch in bro_list:
            i[ "ch_type" ] = "broadcast"
        else:
            i[ "ch_type" ] = "err_none"
            
        i[ "producing" ] = ch  in channel_producing 

        res_switch( i )

        a = ( i[ "channel" ]  ,
               i[ "channel_name" ] ,
               i[ "ch_type" ] ,
               i[ "confirmed" ] ,
               i[ "outer_collected" ] ,
               i[ "is_box" ] ,
               i[ "is_web" ] ,
               i[ "is_qingting" ] , 
               i[ "producing" ] , 
               i[ "collect_advice" ] ,
               i[ "stream_info" ] ,
               )
        res = res +  ",".join( a )  + "\n"

    res = res + "bad data both in bro and box ," + ",".join( list(  set( box_collecting_list ) & set( bro_collecting_list ) ) ) + "\n"
    res = res + "producing but not mentioned," + ",".join( list( set( channel_producing )  - set( [ i[ "channel" ] for i in all_ch_list ] ) )) + "\n"
    res =  res+ "广告生产频道数量数量 ," + str( len( channel_producing ) )  +  "\n"
    res =  res+ "\n"
    return res


@try_catch( [] )
def _producting_ch_list():
    return [ i[ "channel" ] for i in  db.query( "select channel from edit_lock where enable in  (  1,2 )  " , "db" ) ] 

def producting_ch_list():
    ret =  _producting_ch_list()
    if [] == ret :
        return json.dumps( { "code" : 0 , "msg": "ok" , "data" : ch_list } ) 
    return json.dumps( { "code" : -1 , "msg": str( err )  } )  


def collecing_stream_info( ch_list = [] ):
    try :
        ch_stream_id_map = {}
        res = {}
        ch_filt_str = "1=1"
        if len( ch_list ) != 0 : 
            ch_filt_str = " channel_inner_id in ( %s )"  %  ",".join( [ "'%s'" % i for i in ch_list  ] )
        for i in db.query( "select channel_inner_id , stream_point_id , status , score ,break_perc from t_channel where %s  and status >=0  " % ch_filt_str , "ts_stream" ) : 
            ch_stream_id_map[ i[ "channel_inner_id" ]  ] = i[ "stream_point_id" ]
            res[ i[ "channel_inner_id" ] ] =  i 
            i[ "stream_info" ] ={}
            i[ "break_perc" ] = float( "%.1f" %  ( 100 * i[ "break_perc" ]  )  )
            i[ "main_stream_type" ] = "未知"
        sql = """
select 
idx ,channel_inner_id , channel_outer_id  ,  source_id , status , score 
from  t_id_map
where 
channel_inner_id in ( select channel_inner_id from  t_channel )
and 
%s 
and 
status  in( 1 , 5 , 0  )
"""
        stream_data = db.query( sql % ch_filt_str  ,"ts_stream" ) 
        stream_name_req_info = [ { "type" : i[ "source_id" ] , "channel" : i["channel_outer_id"] } for i in stream_data ]
        stream_name_info = stream_name_map_get( stream_name_req_info )
        not_used_source_list = []
        for i in  stream_data:
            ch =  i[ "channel_inner_id" ]
            if ch not in res  :
                not_used_source_list.append(  { "channel_outer_id" : i[ "channel_outer_id"]  ,
                                             "is_main": 1 ,
                                             "source" : i[ "source_id" ]  , 
                                             "status" : i[ "status" ]   ,
                                             "channel_outer_name"  :  stream_name_info.get( "%s_%s" %( i[ "source_id" ] , i[ "channel_outer_id" ] ) , "不知道" )  ,
                                             "channel_inner_id" : ch  })  
                continue

            res[ ch ][ "stream_info" ][ i[ "idx" ] ] =   { "channel_outer_id" : i[ "channel_outer_id"]  ,
                                                  "is_main": 0 ,
                                                  "source" : i[ "source_id" ]  , 
                                                  "status" : i[ "status" ] ,
                                                  "idx" : i[ "idx" ] ,
                                                  "channel_outer_name"  :  stream_name_info.get( "%s_%s" %( i[ "source_id" ] , i[ "channel_outer_id" ] ) , "不知道" )  ,
                                                  "score"  :  i[ "score" ] 
                                                  } 

            if ch in ch_stream_id_map and  i[ "idx" ] == ch_stream_id_map[ ch ] :
                res[ ch ][ "stream_info" ][ i[ "idx" ] ][ "is_main" ] = 1 
                res[ ch ][ "main_stream_type" ] =   i[ "source_id" ] 

        return  { "code" : 0 , "msg": "ok" , "data" : { "collect" :  res , "not_used_stream"  : not_used_source_list } }
    except Exception as err :
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print( err )
        return  { "code" : -1 , "msg": str( err )  } 

def get_switching_ch_list() :
    res = {}
    for i in db.query( "select channel_inner_id ,dest_stream_point_id from t_stream_switch_job  where status != 2 ","ts_stream" )   :
        res[  i [  "channel_inner_id"  ] ]= i [ "dest_stream_point_id" ]
    return res
        

def get_ch_detail( ch_list ,all_data = False ):
    if len( ch_list ) == 0 and not all_data : return ""
    if all_data : 
        streaming_info = collecing_stream_info( [] )[ "data" ][ "collect" ]
        ch_list = streaming_info.keys()
    else:
        streaming_info = collecing_stream_info( ch_list )
        streaming_info = streaming_info[ "data" ][ "collect" ]

    all_producing_list = _producting_ch_list()
    switching_channel_list = get_switching_ch_list() 
    res = {}
    for i in ch_list : 
        res[ i ] = {}
        if i in streaming_info :
            res[ i ][ "score" ] = streaming_info[ i ][ "score" ]
            res[ i ][ "status" ] = streaming_info[ i ][ "status" ]
            res[ i ][ "break_perc" ] = streaming_info[ i ][ "break_perc" ]

            if len( streaming_info[ i ][ "stream_info" ] ) == 0 :
                res[ i ][ "streaming"  ] =  0
            else:
                res[ i ][ "streaming"  ] =  1
            res[ i ][ "status" ] = streaming_info[ i ][ "status" ]

        else:
            res[ i ][ "status" ] = 0
            res[ i ][ "score" ] =  0
            res[ i ][ "streaming" ] = 0
            res[ i ][ "break_perc" ] = -1
        source = []
        if i in streaming_info :
            for j in streaming_info[ i ][ "stream_info" ].values() :
                source.append( j )
                if i in switching_channel_list and switching_channel_list[ i ] == source[ -1 ][ "idx" ]  :
                    source[ -1 ][ "is_main" ] = 2
        res[ i ][ "collect_source" ] = source

        if i in  all_producing_list :
            res[ i ][ "producing" ] = 1
        else:
            res[ i ][ "producing" ] = 0

    return json.dumps( res )

import re
def stream_info_get( ch ):
    tmp = db.query( "select channel_inner_id , channel_outer_id  , source_id ,idx from t_id_map  where channel_inner_id = '%s' and status = 0 " % ch  , "ts_stream" )
    st = time_str_to_ts( ts_to_time_str( time.time() - 86400 )[ 0:10 ] + " 09:00:00"  )
    et = st + 10 * 60
    return [ { "id": i[ "idx" ] , "url" : "http://47.96.182.117/api/getVod?type=%s&id=%s&start=%s&end=%s" %  ( i[ "source_id" ] , i[ "channel_outer_id" ] , st , et  ) }  for i in tmp ]

def stream_switch_job_add( arg ): 
    channel = arg[ "channel" ]
    idx =  int( arg[ "dest_idx" ] )
    clear_start_time = arg[ "clear_start_time" ]
    comment = arg[ "comment" ]
    who = arg[ "who" ]
    res = db.query( "select count( * ) __num__ from  t_id_map where idx = %d and  channel_inner_id = '%s' and status = 0  "  %  (  idx , channel )  , "ts_stream")[ 0 ][  "__num__" ]
    if res != 1 : return  { "ret": -1 , "msg" : "数据流不匹配频道" }
    if time.time() - time_str_to_ts( clear_start_time ) > 86400 * 30 : 
        return  { "ret": -1 , "msg" : "清理过多数据" }
    res = db.query( "select count( * ) __num__ from  t_stream_switch_job where  channel_inner_id = '%s' and status = 0  "  %  ( channel ) ,"ts_stream")[ 0 ][  "__num__" ]
    if res != 0 : return  { "ret": -1 , "msg" : "正在切换 不可修改" } 
    ret  = db.query( "insert into t_stream_switch_job (channel_inner_id , dest_stream_point_id, clear_start_time , comment ,who ) values(  '%s' ,%d , '%s' ,  '%s' , '%s' )" % ( channel , idx , clear_start_time , comment ,who  ) , "ts_stream" )
    db.query( "update t_channel set status = 2 where channel_inner_id = '%s'" % ( channel )  , "ts_stream" ) 
    return  { "ret": 0 , "msg" : "添加成功" } 

def play_back( idx, start ,end  ): 
    ret = db.query( "select channel_outer_id , source_id from t_id_map  where idx = %d " % idx , "ts_stream"  )[ 0 ]
    return  "http://47.96.182.117/api/getVod?id=%s&start=%s&end=%s&type=%s"  % ( ret[ "channel_outer_id" ] , start , end , ret[ "source_id" ]  )

def stream_attach( idx, channel_inner_id ): 
    if "" != channel_inner_id and channel_inner_id != 'a0001':
        res = db.query( "select count( * ) __num__ from  t_id_map where  channel_inner_id = '' and idx = %d and status = 0  "  %  ( idx ) ,"ts_stream")[ 0 ][ "__num__" ]
        if 1 != res :
            return { "ret" : -1 , "msg" : "此流已经被分配"}
    #para check 
    db.query( "update t_id_map  set channel_inner_id = '%s' where idx = %d  " %( channel_inner_id , idx )  , "ts_stream" )
    return { "ret" : 0 }

def free_stream_get(): 
    res = db.query( "select idx ,  source_id source , channel_outer_id from  t_id_map where  channel_inner_id = '' and status = 0 " , "ts_stream")
    stream_name_req_info = [ { "type" : i[ "source" ] , "channel" : i["channel_outer_id"] } for i in res ]
    stream_name_info = stream_name_map_get( stream_name_req_info )
    for i in res :
        i[ "channel_outer_name" ] = stream_name_info.get( "%s_%s" %( i[ "source" ] , i[ "channel_outer_id" ] )  ) 
    return  list( res )

@try_catch( {} )
def stream_name_map_get( ch_info_list ): 
    req_para = {}
    for i in ch_info_list: 
        if i[ "type" ] not in req_para :
            req_para[ i[ "type" ] ] = [ i[ "channel" ] ]
        else:
            req_para[ i[ "type" ] ].append( i[ "channel" ] )
    ret = {}
    for source_id  in req_para :
        para = "channel=%s&type=%s" % (  json.dumps( list( req_para[ source_id ] ) ) , source_id )
        req_return = url_req( "http://47.96.182.117/manage/index/getChannelSourceName" , post_str = para ,timeout_sec = 1 , try_count = 2  )
        res = json.loads( req_return )[ "data" ]
        for i in res : 
            ret[ "%s_%s" % ( source_id, i )  ] = res[ i ]
    return ret

def err_collet_ch_list(): 
    res = db.query( """
            select  t_channel.channel_inner_id , t_channel.channel_cname 
            from  t_channel 
            left join 
            t_id_map
            on 
            t_channel.stream_point_id = t_id_map.idx 

            where t_id_map.status = -1 and t_channel.status  =1  
            and t_id_map.channel_inner_id not in ( select channel_inner_id from t_stream_switch_job where status = 0  )
""", "ts_stream" )
    if len( res ) == 0 : 
        return []
    return  res
    
def add_advice_for_bad_sample( channel , ad_uuid , adv ):
    db.query( "update  t_sample_clear_log set editor_advice = '{}' where channel = '{}' and uuid = '{}' ".format( adv , channel , ad_uuid ) , "db_err_log")
    db.query( "update  `uuid`  set editor_advice = '{}' where fid = '{}' and uuid.uuid = '{}' ".format( adv , channel , ad_uuid ) , "db_bad_sample")

def channel_produce_insert( channel_info ): 
    ch = channel_info[ "channel" ] 
    res = db.query( "select 1 from edit_lock where channel = '%s'  "  % ( ch ), 'db' )
    if res and len( res )  == 1  :
        sql = 'update edit_lock set channel_cname = "%s" , ip = "%s" , domain_name = "%s"  , enable ="%s" ,  type = "%s" , `group` = "%s" , priority = "%s" where channel = "%s" limit 1  ' 
        sql = sql % (channel_info[ "channel_cname" ]  , channel_info[ "ip" ] , channel_info[ "domain_name" ] , int( channel_info[ "enable" ] ) , int( channel_info[ "type" ] ) , int( channel_info[ "group" ] ) , int(  channel_info[ "priority" ])  , ch )
    else :
        sql = "insert into edit_lock ( channel , channel_cname , ip ,domain_name , enable , type , edit_lock.group , priority , status , create_time ) values( '%s' ,'%s' ,'%s' ,'%s' ,  %s ,%s , %s , %s , 0  ,now() )" % ( channel_info[ "channel" ] ,
        channel_info[ "channel_cname" ]  ,
        channel_info[ "ip" ] ,
        channel_info[ "domain_name" ] ,
        int( channel_info[ "enable" ] ) ,
        int( channel_info[ "type" ] ) ,
        int( channel_info[ "group" ] ) ,
        int(  channel_info[ "priority" ])  )
    ret = db.query( sql , "db" )
    if None == ret :
        return -3 , "频道添加失败"
    return  0 , "频道添加成功"
        

import uuid
def empty_task_insert( channel , st ):
    st = st - 86400 * 5
    sql = "insert into task (task_uuid, channel, start, start_time, end, end_time, path, status, create_time) values ('%s','%s','%s',from_unixtime('%s'),'%s',from_unixtime('%s'), '', '0',now())" %( str( uuid.uuid4() )  , channel , st , st , st , st  )
    ret = db.query( sql , "db" )
    if None  == ret :
        return -1 , "数据库连接失败"
    sql = "update edit_task set edit_status = -2 where channel = '%s' and  edit_status = 0  and start < %s   " %( channel , st )
    ret = db.query( sql , "db" )
    if None  == ret :
        return -1 , "数据库连接失败"
    return 0 , "插入成功"

def seg_join( a, b  , a1 ,b1 ):
    if b <  a1 or  b1 < a  : return ( None , None  )
    if a1 <= a and b <= b1 : return ( a , b )
    if a <= a1 and b1 <= b : return ( a1 , b1  )
    if a1 <= a and a <= b1 : return ( a , b1 )
    if a1 <= b and b <= b1 : return ( a1 , b )

def collect_summary( ch ,start_date , end_date ):
    st = int( time_str_to_ts( start_date + " 00:00:00" ) )
    et = int( time_str_to_ts( end_date + " 00:00:00" ) )
    if st == et : 
        et = st +  86400
    if ( et - st ) % 86400 > 31  : 
        et = st + 86400 * 31 
    gap_list = json.loads( url_req( "http://az.hz-data.xyz:8087/channel/%s/m3u8.gap?start_time=%s&end_time=%s" % ( ch ,st ,et  ) ,timeout_sec = 1 , try_count = 2 ) )[ "data" ] 
    res = []
    for i in range( st , et , 86400  ):
        cur_gap_list =  filter( lambda x : x [ 1 ] != None  ,  [ seg_join( i , i + 86400 , x[ "start" ] , x[ "end" ] ) for x in gap_list ] ) 
        gap_sum = sum( [ x[ 1 ] - x[ 0 ] for x in cur_gap_list ] )
        res.append( { "date" : ts_to_time_str(  i  , "%Y%m%d" ) , "gap_perc" : "%.4lf" % ( gap_sum / 86400.0 )  } )
    return res

def collect_summary_list( ch_list ,start_date , end_date ):
    if start_date > end_date : return   -1 ,  []
    return 0 ,  [ {  "channel": i ,  "gap_perc" : collect_summary( i , start_date ,  end_date ) } for i in ch_list  ]

def vs_gap_summary( ch , s_date ,e_date ):
    st = int( time_str_to_ts( s_date +  " 00:00:00" ) )
    et = int( time_str_to_ts( e_date +  " 23:59:59" ) )
    gap_info = json.loads( url_req(   "http://az.hz-data.xyz:8087/channel/%s/m3u8.gap?start_time=%s&end_time=%s" % ( ch ,st,et ) ,timeout_sec = 1 , try_count = 2  )  )[ "data" ]
    res = []
    for i in range( st ,et , 86400 ) :
        res.append( {  "date" : ts_to_time_str( i ,  "%Y-%m-%d" ) , "gap_perc" : int(  sum( [ seg_join_length( x[ "start"  ] , x[ "end" ] ,  i , i+ 86400 )  for x in gap_info ] ) / 86400.0 * 10000 ) / 100.0 } )
    return res

def get_score_of_channel( ch ):
    st ,et =  get_last_day_start_end()
    time_gap_list =list( range( int( st  ) ,  int( et ) , 3 *  3600 ) ) +[ int( et ) , ]
    gap = 0
    for i in range( len(  time_gap_list ) - 1 ) :
        cur_st = time_gap_list[ i ]
        cur_et = time_gap_list[ i + 1 ]
        tmp =  url_req( "http://az.hz-data.xyz:8087/channel/%s/playback.m3u8?start_time=%s&end_time=%s" % ( ch , cur_st , cur_et  ) ,timeout_sec = 1 , try_count = 2 )
        url_ts_list = [( cur_st,cur_st )  ] +   [  get_s_e( line ) for line in tmp.split("\n") if line.startswith( "http://")  ]  +   [ ( cur_et, cur_et  )   ]
        gap = gap +  sum( [ url_ts_list[ i + 1 ][ 0 ]  - url_ts_list[ i ][ 1 ] for i in range( len( url_ts_list ) - 1 )   if url_ts_list[ i + 1 ][ 0 ]  - url_ts_list[ i ][ 1 ] > 1  ] )
    score = gap_to_score( (float( gap  ) / 86400.0 ) )
    db.query( "update t_channel set score = %d  where channel_inner_id = '%s'"  %( score , ch ) , "ts_stream" )

def mark_audio_notify_rsp( mark_id , status ) :
    db.query( "update bromoni set watcher_rsp  = %s where remote_id = %s " %( status , mark_id )  , "ch_check")

def audio_mark_recheck( ch , source_ch ,source_id ) :
    check_url = "http://47.96.182.117/api/getVod?id={}&start=%s&end=%s&type={}".format( source_ch , source_id ) 
    ret =  db.query( "select sample_info , scale  from brochannelinfo where channel = '%s' " %( ch ) , "ch_check") 
    sp_info =  json.loads( ret[ 0 ][ "sample_info" ] )
    sp_play_ts = time_str_to_ts(  ts_to_time_str( last_day_begin() , "%Y-%m-%d " ) + ret[ 0 ][ "scale" ] )
    sp_recheck_url = check_url % ( sp_play_ts - 30 , sp_play_ts + 30 )
    sp_info.append( sp_recheck_url )
    play_info = [ { "start" : i , "end" : i + 3600 , "show_start" : ts_to_time_str( i ) , "show_end" : ts_to_time_str( i + 3600 ) , "play_url" :  check_url % ( i , i+ 3600  ) }  for i in range(  int( last_day_begin() ) , int( today_begin() ), 3600   )   ]
    return {  "a_ch_mark" : sp_info , "play_info": play_info  } 

def stream_ts_get( channel_id , start ,end  ) :
    wh = "where channel_inner_id = '%s' and start between %s and %s and end between %s and %s" %( channel_id , start - 8000 , end  , start , end + 8000 )
    ret = db.query( "select max( update_time  ) as tm from t_stream_data %s "  % wh , "ts_stream" )
    if None != ret[ 0 ][ 'tm' ] : 
        return  int( ret[ 0 ][ "tm" ].strftime( "%s" ) )
    return start - 3 * 86400

from tools.math import cp_and_join
def stream_source_detail( ch , start , end  ) :
    def data_join( x , y ):
        x[ "end" ] = y[ "end" ]
        x[ "update_time" ] = max( y[ "update_time" ] ,  x[ "update_time" ] ) 
        return x
    sql_wh = "channel_inner_id = '%s' and (  start between %s and %s ) and ( end between %s and %s )  " % ( ch,  start - 4000, end , start ,end +  4000  )
    ret = db.query( "select start ,end  , source_id as type , channel_outer_id as id , date_format( update_time  , '%%Y-%%m-%%d %%h:%%m:%%s' ) as update_time from t_stream_data where %s order by start  " % sql_wh , "ts_stream" )
    ret = cp_and_join( lambda x , y :x[ "id" ] == y[ "id" ] and x[ "type" ] == y[ "type" ] and y[ "start" ] - x[ "end" ] < 5 , data_join , ret  )
    if len( ret ) == 0 : return []
    ret[ 0 ][ "start" ] = max( start , ret[ 0 ][ "start" ] )
    ret[ -1 ][ "end" ] = min( end , ret[ -1 ][ "end" ] )
    for i in ret :
        i[ "start_date" ] = ts_to_time_str( i[ "start" ] )
        i[ "end_date" ] = ts_to_time_str( i[ "end" ] )
    return ret

@try_catch( ( -1 , "" ) )
def channel_id_from_outer_to_inner( channel_outer_id , source_id ) :
    return  0 , db.query( "select channel_inner_id  from t_id_map where channel_outer_id = '%s' and source_id =%s " %(channel_outer_id , source_id ) ,"ts_stream" )[ 0 ][ "channel_inner_id" ]

#@fc_test( '' , '11000010015092'  , '1' )
def channel_id_from_outer_to_inner_call( self ,  channel_outer_id , source_id ) :
    ret_write( self ,  * channel_id_from_outer_to_inner( channel_outer_id , int( source_id  ) )   )

#@fc_test( '' )
def channel_id_from_outer_to_multi( self ) :
    ch_list =json.loads(  self.get_argument( 'source_list' ) )
#    ch_list = [  ['11000010015092'  , '1'  ] ]
    ret = []
    for x  in ch_list:
        tmp = channel_id_from_outer_to_inner( x[ 0 ] , int( x[ 1 ]  ) )
        ret.append(  '??' if -1 == tmp[ 0 ] else  tmp[ 1 ]) 
    ret_write( self , 0, ret )

#print( get_ch_detail( [ "11000010006875" ] ) )
#print( stream_source_detail( "11000010003179"  , 1561910400.0 , 1564588800.)  )
#channel_produce_insert( { 'channel' :  'java', "channel_cname"   :"what!!!"  , "ip"  :"1.1.1.1" , "domain_name"  : "2.2.2.2" , "enable"  : 2 , "type"  : 2  , "group" : 12 , "priority"  :111 } )
