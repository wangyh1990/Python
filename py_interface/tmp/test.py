#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#from local_utils import *
#from tools.network  import * 
#from tools import db
#from tools.sql_mk import where_in
#from tools.math import *
#from tools.utils import *
#from local_utils  import _err_ansi_range
#import json
#import copy
#import threading
#
#
#def summary_cross_1( ch , start , end , self_start = -1 , self_end = -1 ):
#    def _cross_check(  data_range , summary_join_fc ):
#        data_range = 1800
#        sql_cmd = "select start ,end from summary where channel = '%s' and ( start between %d and %d )  and ( end  between %d  and %d   ) and tag in ( 0 ,1  ) and duration < 300  and start != %s and end != %s "%  ( ch ,  start - data_range , end + data_range  , start - data_range , end + data_range )
#        res_list = db.query( sql_cmd , "db" ) 
#    if start >= end :
#        return False
#    return _cross_check( 1800 , summary_seg_join  ) if end - start < 300 else _cross_check(  3800 , summary_seg_join_long )
#
#
#def summary_alter( channel ,start , end , new_start ,new_end ):
#    #check if summary exist
#    if summary_cross_1( channel , new_start ,new_end , self_start =  start , self_end = end ):
#        return -1 , '您所修改的位置和其他的播放重复了'
#    sql_form  = '''
#        update summary set start = %s , end = %s , start_time = '%s' , end_time ='%s'  , score = 75.01 where channel = '%s' and start = '%s' and end = %s and tag in ( 0 , 1 , 3 )
#    '''
#    db.query( sql_form %( new_start , new_end , ts_to_time_str( new_start ), ts_to_time_str( new_end )  , channel , start , end ) ,'db' )  
#    return 0 , 'done' 
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#def summary_alter( channel ,start , end , new_start ,new_end ):
#    #check if summary exist
#    if summary_cross_1( channel , new_start ,new_end , self_start =  start , self_end = end ):
#        return -1 , '您所修改的位置和其他的播放重复了'
#    sql_form  = '''
#        update summary set start = %s , end = %s , start_time = '%s' , end_time ='%s'  , score = 75.01 where channel = '%s' and start = '%s' and end = %s and tag in ( 0 , 1 , 3 )
#    '''
#    db.query( sql_form %( new_start , new_end , ts_to_time_str( new_start ), ts_to_time_str( new_end )  , channel , start , end ) ,'db' )  
#    print( summary_delete_notify( channel, start )  , 'here' )
#    return summary_insert_notify( channel , new_start , new_end , sp_uuid   75.01 )
##    query 要支持返回多行
#
#
#def summary_insert_notify( channel , start , end ,sp_uuid ,score ):
#    tmp = { "mediaId" : channel , "issueList" : [{  "start" : start , 'end' :end , 'uuid' :sp_uuid , 'score': score  }   ] }  
#    url = "http://dmp.goldclippers.com/Open/Ai/add_issue?person_code=100101&person_password=e10adc3949ba59abbe56e057f20f883e"
#    res = js_load_req( url  ,  post_str = json.dumps(tmp) , try_count = 111  )#细化错误检查，失败了怎么办？超时了怎么办 load失败了怎么办
#    return ( 0 , 'ok' )if ret[ 'count' ] !=  0 ( -1 , '远端更新失败' )
#
#def summary_alter( channel ,start , end , new_start ,new_end ):
#    #check if summary exist
#    if summary_cross( channel , new_start ,new_end , self_start =  start , self_end = end ):
#        return -1 , '您所修改的位置和其他的播放重复了'
#    sql_form  = '''
#        update summary set start = %s , end = %s , start_time = '%s' , end_time ='%s'  , score = 75.01 where channel = '%s' and start = '%s' and end = %s and tag in ( 0 , 1 , 3 )
#    '''
#    db.query( sql_form %( new_start , new_end , ts_to_time_str( new_start ), ts_to_time_str( new_end )  , channel , start , end ) ,'db' )  
#    print( summary_delete_notify( channel, start )  , 'here' )
#    return summary_insert_notify( channel , new_start , new_end , sp_uuid   75.01 )
##    query 要支持返回多行
#
#def summary_ts_alter( summary_info ):
##def summary_alter( channel , start ,  end ):
#    name_alter
#    name_alter
#    if new_title == '' or  False : #do not exist :
#        pass
##        just change start ,end
#    else:
#        pass
##        just change start ,end
##        do change start ,and do match
#
#ef test():
#    pass
#
#test()


from local_utils import *
from tools.network  import * 
from tools import db
from tools.sql_mk import where_in
from tools.math import *
from tools.utils import *
from local_utils  import _err_ansi_range
import json
import copy
import threading
from tools.time_convert import *

