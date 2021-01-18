from tools.network  import * 
from tools import db
from tools.sql_mk import where_in
from tools.math import *
from tools.utils import *

from local_utils import *

import json


@handler_rsp
def sp_fixed(  idx , auther ):
    db.query( "update uuid set process_status = 2 ,advise = concat( advise , '%s'  ) where id =  %s  limit 1 " %(  '|%s_fix'  %(  auther  ) ,  idx )    ,'db_produce_info' )
    return 0 ,  'done'

def sp_err_summary( self , start_time , end_time ,  query_way ):
    if 'gp' == query_way :
        res = [ { "kw" :  '1' , 'count' : 12  } , { "kw" :  '2' , 'count' : 132  } , { "kw" :  '3' , 'count' : 13  } , { "kw" :  '4' , 'count' : 1  }  ]
    elif 'err_type' ==  query_way :
        res = [ { "kw" :  3 ,
  'count' : 12  } ,
  { "kw" :  4 ,
  'count' : 132  } ,
  { "kw" :  7 ,
  'count' : 13  } ,
  { "kw" :  8 ,
  'count' : 1  }  ,
  { "kw" :  10 ,
  'count' : 1  }  ,
  { "kw" :  5 ,
  'count' : 1  }  ,
  { "kw" :  6 ,
  'count' : 1  }  ,
  { "kw" :  9 ,
  'count' : 1  }   ,
          { "kw" :  11 , 'count' : 1  }  ]
    else :
        res = []
    self.write( json.dumps( { 'code' : 0 ,  'count' :len( res ) , 'data' : res } ) ) 

def sp_err_detail( self, start_time , end_time  ):
    res = [ { 'channel': '11000010004279'  ,
  'channel_name'  : '武威人民广播FM90.8' ,
  'start' : '2019-10-11 12:00:00' ,
  'end' : '2019-10-11 12:00:10' ,
  'dura' :  10 ,
  'title' : '迈巴赫' ,
  'checker' : 'gq' ,
  'auther' : 'edit02' ,
  'process_status' : '已经处理' ,
  'err_reason' : '广告不完整' ,
  'summary_count' : 13 }  ,
   { 'channel': '11000010004279'  ,
  'channel_name'  : '武威人民广播FM90.8' ,
  'start' : '2019-10-11 12:00:00' ,
  'end' : '2019-10-11 12:00:10' ,
  'dura' :  10 ,
  'title' : '迈巴赫' ,
  'checker' : 'gq' ,
  'auther' : 'edit02' ,
  'err_reason' : '广告不完整' ,
  'process_status' : '已经处理' ,
  'summary_count' : 13 } ]
    self.write( json.dumps( {  'total' :len( res ) , 'rows' : res } ) ) 


def sp_left( self , gp  ):
    if int( gp ) == 0 :
        ch_sql_limit = '1=1'
    else :
        ch_sql_limit = 'fid ' +  where_in( get_ch_list_of_gp( int(gp ) ) )

    res = db.query( 'select advise advice  , id , fid channel , channel_cname channel_name , start ,end ,duration dura ,error_id , title  ,error_cause ,summary_count  from uuid where %s and process_status  in( 1 )   and source = 0 and id >  5001  and error_id  in( 3,4  ) limit %s , %s  ' %( ch_sql_limit ,* get_search_range( self ) ) ,'db_produce_info' ) 
    for x in res: 
        try :
            x[ 'err_reason' ] = err_id_2_name.get( x[ 'error_id' ] , " ??不知道" ) 
            x[ 'checker' ] =  x[ 'error_cause' ].split( ';' )[ -1 ].split( ":" )[ -1 ]
        except :
            x[ 'checker' ] =  '??'
        x[ 'start' ] = x[ 'start' ].strftime( '%Y-%m-%d %H:%M:%S' )
        x[ 'end' ] = x[ 'end' ].strftime( '%Y-%m-%d %H:%M:%S' )  
        del x[ 'error_cause' ]
    self.write( json.dumps( { 'code' : 0 ,  'count' :len( res ) , 'data' : res } ) ) 

@para_check( [ 'process_status'  , 'advise' , 'who' ] )
def sp_err_update( self , idx  ):
    new_process_status = self.get_argument( 'process_status' )
    advise = self.get_argument( 'advise' , '' )
    who = self.get_argument( 'who' ,'' )
    db.query( "update uuid set process_status = %s ,advise = concat( advise , '%s'  ) where id =  %s " %( new_process_status , '|%s:%s'  %(  who , advise ) ,  idx )    ,'db_produce_info' )
    ret_write( self ,  0 ,  'done'  )

handler_list = [
        (r'/sp_fixed/(?P<idx>.*)/(?P<auther>.*)/',  sp_fixed ),
        (r'/sp_left/(?P<gp>.*)/',  mk_req_handler2( get = sp_left ) ),
        (r'/sp_err_update/(?P<idx>.*)/',  mk_req_handler2( get = sp_err_update ) ) ,
        (r'/sp_err_summary/(?P<start_time>.*/(?P<end_time>.*))/(?P<query_way>.*)/',  mk_req_handler2( get = sp_err_summary  )  ) ,
        (r'/sp_err_detail/(?P<start_time>.*/(?P<end_time>.*))/', mk_req_handler2( sp_err_detail ) ) ,
        ]
