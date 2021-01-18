from tools import db
from local_utils import *
from tools.sql_mk import *


err_id_name_map  = {3: '剪辑不完整' , 4: '多条剪成一条' , 7 : '串台' , 8:  '画面异常'   }
def removed_sp_list( self , gp ):
    #create a thread to reset all the unprocessed removed sp
    gp = int( gp )
    limit_s, limit_e  = get_search_range( self )
    if gp != 0 :
        ch_list = get_ch_list_of_gp( gp )
        ch_limit_sql = 'fid '+  where_in( ch_list )
    else: 
        ch_limit_sql = '1=1'
    print( limit_s , limit_e )
    field_list = [ 'id'  , 'uuid' , 'fid' , 'error_cause' , 'editor_advice' , 'create_time'  , 'update_time'  , 'source' , 'sp_auther'  , 'processor_list' , 'process_status'  , 'auther' , 'error_id']
    res = db.query( "select %s from uuid  where %s and (  (  source = 0 and error_id  %s ) or source = 1  ) and status = 1 and  id > 472246   order by field( process_status , 0,1,2,3  )  limit %s , %s " %( ','.join( field_list )  ,  ch_limit_sql ,where_in( list( err_id_name_map.keys()  ) )  , limit_s , limit_e) , 'db_bad_sample')
    ch_name_map = get_channel_name_map( list( set( [  x[ 'fid' ]  for x in res   ] ) ) )
    for x in res :
        try :
            x[ 'ch_name' ] = ch_name_map.get( x[ 'fid' ] , "" )
            x[ 'create_time' ] = x[ 'create_time' ].strftime( "%Y-%m-%d %m:%m:%d" )
            x[ 'update_time' ] = x[ 'update_time' ].strftime( "%Y-%m-%d %m:%m:%d" )
            if x[ 'source' ] == 0 :
                x[ 'auther' ] =  x[ 'error_cause' ].split( ';' )[ -1 ]
                x[ 'error_cause' ] =  err_id_name_map.get( x[ "error_id" ] , '处理错误' ) 
                if x[ "error_id" ] ==  -1  :
                    db.query("update  uuid set error_id = %s where id = %s" %( x[ 'error_cause' ],x[ 'id' ] ) , 'db_bad_sample')
        except Exception as err: 
            get_logger().info( "removed_sp_list err  %s " %( err ) )
        x[ 'auther' ] = '' if x[ 'auther'] == 'admin'  else x[ "auther" ]
    ret_write( self, 0 , res )

def removed_summary_of_sp( self , removed_sp_id ):
    return 

handler_list = [ 
(r'/removed_sp_list/(?P<gp>.*)/', mk_req_handler( get = removed_sp_list ) ) ,
#(r'/removed_sp_summary/(?P<gp>.*)/', mk_req_handler( get = removed_sp_list ) ) ,
#(r'/removed_summary_of_sp/(?P<removed_sp_id>.*)/', mk_req_handler( get = removed_summary_of_sp ) ) ,
#(r'/removed_sp_update/(?P<idx>.*)/', mk_req_handler( get = removed_summary_of_sp ) ) ,
#(r'/removed_sp_reclean/', mk_req_handler( get = removed_summary_of_sp ) ) 
]
