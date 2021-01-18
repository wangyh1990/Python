from tools import  db
from local_utils import * 


def epg_remove( self ,  epg_id ):
    epg_id = int( epg_id )
    db.query( "update epgs set  status = 1  where  id = %d " % epg_id , 'tv_epg' )
    ret_write( self, 0 )

@try_catch( "sha?" )
def get_epg_ch( local_ch ):
    return db.query( "select hzid from channels where hz_formal_id = '%s'   and status = 0 " %( local_ch ) ,  'tv_epg' )[ 0 ][ 'hzid' ]
    

def epg_add( channel_id, start , title  ):
    start = int( start )
    title =  title.strip()
    if ''  == title: return -1  ,'名称有问题'
    epg_ch = get_epg_ch( channel_id )
    if 'sha?'  == epg_ch: return -2  ,'频道id有误'
    play_date = ts_to_time_str( start , '%Y-%m-%d' )
    play_start_time = ts_to_time_str( start , '%H:%M:%S' )
    db.query( "insert into epgs ( channel , play_date , play_start_time , play_time_ts , title  , play_end_time ) values (  '%s', '%s', '%s' , %s  , '%s' , '' ) " %( channel_id , play_date, play_start_time , start , title ) , 'tv_epg' )
    return 0  , 'ok'

def epg_modify( ch, start , end  ):
    pass 

handler_list = [
        ( r'/epg_remove/(?P<epg_id>.*)/', mk_req_handler( get = epg_remove )  ) , 
        ( r'/epg_add/(?P<channel_id>.*)/(?P<start>.*)/?P<title>.*)/', mk_req_handler( get = lambda  channel_id, start , title :ret_write(  * epg_add( channel_id, start , title )  ) ) ) ,
        ( r'/epg_modify/(?P<epg_id>.*)/', mk_req_handler( get = epg_remove )  ) , 
        ]

