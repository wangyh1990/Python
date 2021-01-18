from local_utils import * 
from tools import db
from tools.sql_mk import * 
from tools.network import * 
from stream_utils.base import  dmp_notify
from tools.time_convert import * 
import threading
from stream_utils.base import *

source_id_name_map  = { 1 :"云盒" , 3 :"互联网2" ,  4 :"新互联网"} 

status_to_name = {  2 : "" }
def  stream_switch_state():
    res =  db.query( "select * from t_stream_switch_job where create_time >  '%s'  order by id desc " %( ts_to_time_str( time.time() - 15 * 86400 ) ) , "ts_stream" )
    id_list = [ i[ "dest_stream_point_id" ] for i in res ]
    res2 =  db.query( "select channel_outer_id , source_id , idx  from t_id_map where idx %s" % ( where_in( id_list ) ) , "ts_stream" )
    ch_name_map =  get_channel_name_map( [ i[ "channel_inner_id" ] for i in res ] )
    source_name_map = {  i[ "idx"] : "%s %s" % (  i[ "channel_outer_id" ] ,  source_id_name_map.get( i[ "source_id" ] , "??" ) )  for i in res2 }
    ret = [ "%s %s  >>  [%s, %s]  [%s %s] %s " % ( i[ "channel_inner_id" ] , ch_name_map.get( i[ "channel_inner_id" ]  , "???" )  , source_name_map.get( i[ "dest_stream_point_id" ] , "???" ) , i[ "clear_start_time" ].strftime( "%Y-%m-%d %H:%M:%S" ) , i[ "who" ] , i[ "create_time" ],  status_to_name.get(  i[  "status" ],"未完成" ) ) for i in res  ]
    return ret

def not_confirmed_range_switch_list() :
    res= db.query( "select id , check_ts ,    channel_inner_id , start ,end  , status , produce_status  from t_stream_freeze_range where  ( status = 0 and id  >%s and check_ts not in ( 0, -1 ) ) or (  status = 1 and produce_status = 0  ) order by status " %( 47320  ) , "ts_stream" )
    #res= db.query( "select id , check_ts ,    channel_inner_id , start ,end  , status , produce_status  from t_stream_freeze_range where  ( status = 0 and end >  %s and check_ts not in ( 0, -1 ) ) or (  status = 1 and produce_status = 0  ) order by status " %( last_day_begin() ) , "ts_stream" )
    ch_list =  [ i ["channel" ] for i in db.query( "select channel from edit_lock where enable != 0   " , "db" ) ]

    for i in res :
        st  , et =  i[ "start" ] ,  i [ "end" ]
        ts  = time_str_to_ts(  ts_to_time_str( ( st + et )  / 2 , "%Y-%m-%d %H:00:00" ) )
        i[ "start" ] = i[ "check_ts" ] - 60
        i[ "end" ] = i[ "check_ts" ] + 180
        if i[ "status" ] == 1 and i[ "produce_status" ] == 0  :
            i["note" ] = "生产数据可能有问题"
        else:
            i["note" ] = "machine"
    ch_name_map = get_channel_name_map(  [ i[ "channel_inner_id" ] for i in res  ] )

    data = {
    "code": 0,
    "msg": "成功",
    "dataList": [
    {
    "id": i[ "id" ] ,
    "channel":  i[ "channel_inner_id" ] ,
    "channel_name":  ch_name_map.get( i[  "channel_inner_id"  ] , "??" ) , 
    "e_start": i [ "start" ] ,
    "e_end": i[ "end" ] ,
    "e_start_time": ts_to_time_str(  i[ "start" ] ) ,
    "e_end_time":  ts_to_time_str(  i[ "end" ]) , 
    "start": i [ "start" ] ,
    "end": i[ "end" ] ,
    "submit_user": "xjuser2",# 提交人
    "create_time": "2019-04-16 14:28:50",
    "update_time": "2019-04-22 09:00:20",
    "error_type": 9, #断流 7, or what I get
    "is_exception": 0 , #not used
    "is_check": i[ "status" ] , # 0 undone 1 good 2 bad
    "handle_user": i[ "note" ], #处理人
    "finish_handle_user": i[ "note" ],#最后一次处理人
    "type": -1,#处理方式？红外云盒等
    }
    for i in res if i[ "status" ] == 0  or (   i [ "status" ]  ==  1 and i[ "channel_inner_id" ]  in ch_list )
    ]
    }
    return  data

def range_switch_confirm( data_id  , confirm_status ) :
    db.query( "update t_stream_freeze_range set status= %s  where  id = %s " % ( confirm_status , data_id ) , "ts_stream" )
    def info_update() :
        if 1 == int(  confirm_status):
            res = db.query( "select * from t_stream_freeze_range where id = %s " % ( data_id ) , "ts_stream" )[ 0 ]
            url_req( "http://47.96.182.117/index/rewriteGatherChannel", post_str = 'channel=%s&date=%s' % ( res[ "channel_inner_id" ] , ts_to_time_str( res[ "start" ] , "%Y-%m-%d" ) ) ,timeout_sec = 1 , try_count = 2 )
            url_req( "http://47.96.182.117/index/rewriteGatherChannel", post_str = 'channel=%s&date=%s' % ( res[ "channel_inner_id" ] , ts_to_time_str( res[ "end" ] , "%Y-%m-%d" ) ) ,timeout_sec = 1 , try_count = 2 )
            dmp_notify( res[ "channel_inner_id" ] , res[ "start" ]  ,res[ "end" ] )
            get_logger().info( "update done %s %s " %( data_id , confirm_status ) )
    threading.Thread( target = info_update ).start()
    return 0

def stream_refill_fail(): 
    res = db.query( "select id , channel_inner_id  ch , start ,  end  from t_stream_freeze_range where start > %s and status = 3  and end - start >= 7200  group by start  order by start    " %( time.time() - 86400  *7 ) , "ts_stream" )
    ret = {}
    for i in res : 
        if i[ "ch" ] not in ret : ret[ i[ "ch" ] ]  = []
        ret[ i[ "ch" ] ].append( {  "start" : ts_to_time_str( i[ "start" ] ), "end" : ts_to_time_str( i[ "end" ] )  } )
    return ret 

handler_list = [  (r'/switch_state.json', mk_req_handler( get = lambda self  : ret_write( self , data = stream_switch_state() ) ) ) ,
                  (r'/not_confirmed_range_switch_list', mk_req_handler( get =  lambda self : self.write( json.dumps( not_confirmed_range_switch_list() ) ) )), 
                  (r'/stream_refill_fail', mk_req_handler( get = lambda self  : ret_write( self , data = stream_refill_fail() ) ) ) ,
                  (r'/range_switch_confirm/(?P<data_id>.*)/(?P<confirm_status>.*)/', mk_req_handler( get =  lambda self , data_id , confirm_status  : ret_write( self , ret =  range_switch_confirm( data_id  ,confirm_status )) ) )   ,
]

