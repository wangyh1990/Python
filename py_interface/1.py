from tools.network import *  
from tronado_server import  create_server
from tools import db

def job_done( channel , day  ):
    res  =db.query( "select 1 from epg_status where channel  = '%s'  and date ='%s' " %( channel ,day  ) , 'data_clear' )
    return len( res ) != 0 

def sm_list_get( channel ,day  ):
    res = js_load_req( "http://dmp.goldclippers.com/Open/Ai/get_media_issue" , post_str = "mediaId=%s&date=%s" %( channel ,day   ))  [ "issueList" ]
    res = [  { 'mediaId' : channel , 'startTime' : int( x[ 'start' ] ) } for x in res  ]
    return res

def sm_list_rm( channel ,day  ):
	try :
	    res = url_req( "http://dmp.goldclippers.com/Open/Ai/batch_del_issue" , post_str =  json.dumps( { "delDataList": sm_list_get( channel, day  ) } ) )  
	    db.query( 'delete from   qj_push where fmedia_id  =  "%s" and date =  "%s" limit 1  '  %( channel , day )   , 'qj' )
	except :
	    pass 

