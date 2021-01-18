from local_utils import *
from tools.network import *
from tools.log_factory import get_logger
from tools import db
from tools.sql_mk import where_in
from tools.utils import *
from local_utils import _group_by
import json
import copy
import threading
import datetime

_stream_status_map = {  0 : '未检查',  1 : '正常' ,  2: '异常' , 3 :  '交叉校验异常' }
_ch_status_map = {  0 : '无呼号'  , 1 : '呼号存在'}

def map_map( data , convert_data , source_kw , dest_kw = None , def_val = None ):
    dest_kw = source_kw if dest_kw == None else dest_kw
    for x in data  : 
        x[ dest_kw ] = convert_data.get( x[ source_kw ] , def_val )

def get_edit_task(self ):
    limit_start ,limit_end  = get_search_range( self )
    res = db.query( "select channel , channel_cname  , status ,stream_status   from brochannelinfo where enable  = 1  order by show_level desc  limit %s , %s " %( limit_start , limit_end ) , 'ch_check' )
    if len ( res  ) == 0  :
        ret_write(self , 0 ,  [] )
        return 
    checked_ch_list = [  x['channel' ]for x in res  ]
    res2 = db.query( "select channel ,  channel_outer_id , source_id , status  from brostatus where channel %s and status = 0   " %( where_in( checked_ch_list ) ) , 'ch_check' ) 
    res2 =  _group_by( res2 ,lambda x : x[ 'channel' ] )

    map_map( res, _ch_status_map , 'status' ,  def_val =  '??'  )
    for x in res :
        x['streams'] =  res2.get( x[ 'channel' ] , [] )
        map_map( x[ 'streams' ], _stream_status_map , 'status' ,  def_val =  '??'  )
        for y in x[ 'streams' ]:
            del y[ 'channel' ]
    ret_write(self , 0 , res )

def mark_info(self, channel_id):
    info = db.query( "select  id , name  ,  video_path , status , is_active from  brochannelmark3 where channel = '%s' order by status desc  " %(channel_id ) , 'ch_check' )
    ret_write(self , 0 , info )

def mark_info_update(self, idx , status):
    db.query( "update  brochannelmark3 set status  = %s  where id = %s  limit 1  " %( int( status ) , idx ) , 'ch_check' )
    res =  db.query( " select channel ,count( * ) n  from brochannelmark3  where channel  in ( select channel from brochannelmark3 where id = %s) and status = 1  " %( idx ) , 'ch_check' )[ 0 ]
    db.query( "update brochannelinfo set status = %s  where channel = '%s'   limit 1 " %( res[ 'n'  ] != 0 , res[ 'channel' ]  ) , 'ch_check' )
    ret_write(self , 0 )

def checked_ch_list( self ) :
    res = db.query( "select channel from  tvchannelinfo  where status = 1  "  , 'ch_check' )
    for x in res : 
        x['type'] = 'tv'
    res2 = db.query( "select channel  from  brochannelinfo  where enable = 1 and status  = 1  "  , 'ch_check' )
    for x in  res2:
        x['type'] = 'bro'
    res2 = res + res2

    ch_name_map = get_channel_name_map( [ x[ 'channel' ] for x in res2 ] )

    for x in res2 : 
        x[ 'channel_cname' ] = ch_name_map.get( x[ 'channel' ] , '??')

    ret_write( self , 0 , res2 )

handler_list = [
    (r'/bro_check/ch_list/', mk_req_handler(get=get_edit_task)),
    (r'/bro_check/mark_info/(?P<channel_id>.*)/', mk_req_handler(get=mark_info)),
    (r'/bro_check/mark_info_update/(?P<idx>.*)/(?P<status>.*)/', mk_req_handler(get=mark_info_update)),
    (r'/stream_check/checked_ch_list/', mk_req_handler(get=  checked_ch_list )),
]
