import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options
from local_utils import * 
import new_produce
from tools.time_convert  import * 
from produce import *
import removed_sp_process 
from tools import db
import json
from full_data_collect import *
from tmp_show import *
import traceback 
import bro_mark_check
import stream_service
import os,sys
import new_produce_for_fast 
import err_check
import epg

class DayRandHandler( LocalRequestHandler ):
    def get(self):
        print( 1231 )
        res = self.get_argument( "chosen_day"  , "") 
        if  "" == res :
            chosen_day = []
        else:
            chosen_day = [ int( i ) for i in res.split( "," ) ]

        self.write( random_list_make( int(self.get_argument( "rand_count" ) ) ,
                                      int(self.get_argument( "month_count" ) ) 
                                        , chosen_day ) ) 

    def write_error(self, status_code, **kwargs):
        print( self )
        if self.settings.get("serve_traceback") and "exc_info" in kwargs:
            # in debug mode, try to send a traceback
            for line in traceback.format_exception(*kwargs["exc_info"]):
                self.write(line)
            self.set_header('Content-Type', 'text/plain')
            self.finish()
        else:
            self.finish("<html><title>%(code)d: %(message)s</title>"
                        "<body>%(code)d: %(message)s</body></html>" % {
                            "code": status_code,
                            "message": self._reason,
                        })
        err_msg = ""
        if self.get_status() < 400:
            log_method = get_logger().info
        elif self.get_status() < 500:
            log_method = get_logger().warning
        else:
            log_method = get_logger().error
            err_msg = "\n" + "".join( traceback.format_exception(*kwargs["exc_info"]) )
        request_time = 1000.0 * self.request.request_time()

        print( "we are here" )
        log_method("%d %s %s (%s) %s %s %.2fms%s",
                   self.get_status(), self.request.method,
                   self.request.uri, self.request.remote_ip,
                   self.request.headers["User-Agent"],
                   self.request.arguments,
                   request_time , err_msg)

class ChannDetailRep( LocalRequestHandler ):
    def get(self):
        rep = ch_detail_rep_get()
        self.set_header('Content-Type', 'application/text/plain')
        self.set_header('Content-Disposition', 'attachment; filename=full_rep.csv')
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.write( "\ufeff" +  rep )

class ProductingChList( LocalRequestHandler ):
    def get(self):
        self.write( producting_ch_list() )

class ChannlCollectSourceList( LocalRequestHandler ):
    def get(self):
        self.write( collecing_stream_info() )
    def post(self):
        self.write( collecing_stream_info( ch_list =  json.loads( self.get_argument( "ch_list" ) ) )  )

class ChannelProcessDetail( CrossDomainHandler2 ):
    def post(self):
        try : 
            if 1 == int(  self.get_argument( "all" , default= "-1") ) :
                self.write( get_ch_detail( [], all_data = True  )  )
            else:
                self.write( get_ch_detail( json.loads( self.get_argument( "ch_list" ) ) )  )
        except Exception as err :
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print( err )

class ChannelStreamInfo( CrossDomainHandler ):
    def get(self , channel_id):
        self.write( json.dumps( stream_info_get( channel_id )  ) )

class StreamSwitch( CrossDomainHandler ):
    def get(self):
        arg = {}
        for i in [ "channel" , "dest_idx" , "clear_start_time"  , "comment" , "who" ] : 
            arg[ i ] = self.get_argument( i , -1  )
            if -1 == arg[ i ] :
                self.write( json.dumps( { "ret" : -1  , "msg" : "para err" } ) )
                return 
        self.write( json.dumps( stream_switch_job_add( arg )  ) )

class VStreamPlayBack( CrossDomainHandler ):
    def get(self):
        start = int(  float( self.get_argument('start_time', -1) ) )
        end = int(  float( self.get_argument('end_time', -1) ) )     
        idx = int(  self.get_argument('idx',  -1)  )
        self.redirect( play_back( idx, start ,end  ) )

class StreamAttach( CrossDomainHandler2 ):
    def get( self ):
        idx = int( self.get_argument('idx',  -1) )
        channel =  self.get_argument('channel',  "what") 
        if -1 == idx or channel == "what" :
            self.write( json.dumps( { "ret" : -1  , "msg" : "para err" } ) )  
        self.write( stream_attach( idx , channel  ) )

class ChannelProduceModify( CrossDomainHandler ):
    def get( self ):
        ret , msg = channel_produce_modify( self.get_argument( "channel" ) ,
                                            self.get_argument( "modify_part" ) ,
                                            self.get_argument( "value" )  )
        self.write( json.dumps( {  "ret" : ret , "msg" :  msg } ) )

class ChannelCloudCutStatus( CrossDomainHandler ):
    def get( self ):
        self.write( json.dumps( {  "ret" : 0 , "data" :  ch_cloud_cut_status( self.get_argument( "channel" ) ) } ) )
    def post( self ):
        self.write( json.dumps( {  "ret" : 0 ,
                    "data" :  ch_list_cloud_cut_status(  json.loads( self.get_argument( "ch_list" , "[]" ) ) ) }  ) )

class ChannelProduceStatus( CrossDomainHandler ):
    def post( self ):
        try :
            ret , data = channel_produce_status( ch_list =  json.loads( self.get_argument( "ch_list" , "[]" ) ) , 
                                                 produce_sys =  self.get_argument( "sys"  , "" )   )  
            self.write( json.dumps( {  "ret" : ret , "data" :  data } ) )
        except Exception as err :
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print( err )

    def get( self ):
        ret , data = channel_produce_status( )  
        self.write( json.dumps( {  "ret" : ret , "data" :  data } ) )

class ChannelProduceAdd( CrossDomainHandler ):
    para = { "channel" : None , "channel_cname" :  None  , "ip"  : None , "domain_name" : None , "enable" : None  , "type" : None , "group" : None , "priority" :  None }
    def para_check( self ):
        for i in self.para.keys():
            tmp = self.get_argument(  i ,"what?" )
            if tmp == "what?" : 
                return False
            self.para[ i ] = tmp
        return True

    def get( self ):
        if not self.para_check():
            self.write( json.dumps( {  "ret" : -1 , "msg" :  "参数不够"} ) )
            return
        ret , msg = channel_produce_insert( self.para )
        self.write( json.dumps( {  "ret" : ret , "msg" :  msg } ) )

class BadSampleAdviceAdd( CrossDomainHandler ):
    def get( self ):
        channel = self.get_argument( "channel" )
        ad_uuid = self.get_argument( "uuid" )
        adv = self.get_argument( "advice" )
        add_advice_for_bad_sample( channel , ad_uuid , adv )
        self.write( json.dumps( {  "ret" : 0  } ) )

class CrossDomainHandler( CrossDomainHandler ):
    def get(self):
        data = """
<cross-domain-policy>
<allow-access-from domain="*"/>
</cross-domain-policy>
        """
        self.set_header('Content-Type', 'application/xml')
        self.set_header('Content-Length', '%d' % ( len( data ) ))
        self.set_header('Accept-Ranges', 'bytes')
        self.write(data)

class EmptyTaskInsert( CrossDomainHandler2 ):
    def get( self ):
        ret , msg  = empty_task_insert( self.get_argument( "channel" ) ,
                        int( self.get_argument( "start" ) )  )
        self.write( json.dumps( {  "ret" : ret , "msg" :  msg } ) )


def stream_seg_switch_add( ch , source_id , source_ch ,  start ,end , auther ):
    sql_cmd = "insert into t_stream_seg_switch_job ( channel_inner_id , channel_outer_id , source_id  , start ,end,  auther  ) values ( '%s'  , '%s' , %s , from_unixtime( %s ), from_unixtime( %s ), '%s'  )" 
    db.query( sql_cmd %( ch , source_ch  , source_id  , start , end , auther )  , "ts_stream")

class StreamUpdateTime( CrossDomainHandler ):
    def get( self, channel_id, start ,end  ):
        start , end  = int( start ) , int( end )
        if start >= end : start , end  = end , start 
        self.write( json.dumps( { "ret" : 0 ,  "ts" : stream_ts_get( channel_id ,start , end  ) }) )

def stream_channel( self , channel_outer_id ,  source_id ):
    ret = channel_id_from_outer_to_inner( channel_outer_id , source_id )
    ret_write( self , ret[ 0 ] ,  data = ret[ 1 ] )


def ret_write( self , ret = 0  , data = None  ):
    if None != data :
        self.write( json.dumps( {  "ret" : ret , "data" :  data } ) )
    else:
        self.write( json.dumps( {  "ret" : ret } ) )

def edit_sys_check( self ):
    ret_write( self , 0 ,  data =  ansi_info_get( self.get_argument( "channel" ) , self.get_argument( "date" ) )  )

def sample_to_word( self ,sp_uuid ):
    ret = get_recog_result( sp_uuid )
    if len( ret )  == 0 : 
        ret_write( self , -1 )
    else:
        ret_write( self, 0 , ret )

def cloud_produce_status( self ):
    ch_list = json.loads( self.get_argument( "ch_list" ) )
    ret_write( self , 0 ,  data = produce_finished_point( ch_list )  )

def cloud_produce_efficent( self ):
    ch_list = json.loads( self.get_argument( "ch_list" ) )
    ret_write( self , 0 ,  data = produce_finish_efficent( ch_list ) ) 

def undone_edit_task_count( self ):
    ch_list = json.loads( self.get_argument( "ch_list" ) )
    ret_write( self , 0 ,  data = get_undone_task_count( ch_list ) ) 

def unchecked_sp_count( self ):
    ch_list = json.loads( self.get_argument( "ch_list" ) )
    ret_write( self , 0 ,  data = get_uncheced_sp_count( ch_list ) ) 

def produce_status( self , channel_id ,  date ):
    self.write( json.dumps(  ch_full_status( channel_id , date ))  ) 

def stream_channel( self , channel_outer_id ,  source_id ):
    ret = channel_id_from_outer_to_inner( channel_outer_id , source_id )
    ret_write( self , ret[ 0 ] ,  data = ret[ 1 ] )



class StreamSegSwitch( CrossDomainHandler2 ):
    def get( self, channel_id, start ,end  ):
        if start >= end : start , end  = end , start 
        stream_seg_switch_add( channel_id ,
                               self.get_argument( "source" ) ,
                               self.get_argument( "channel_outer_id" ) ,
                               start ,
                               end ,
                               self.get_argument( "auther" )  )
        self.write( json.dumps( { "ret" : 0 , "msg" : "done" }) )

class CollectSummary( CrossDomainHandler ):
    def post( self ):
        ret , data  = collect_summary_list( json.loads( self.get_argument( "ch_list" ) )  ,
                             self.get_argument( "start_date" ) ,
                             self.get_argument( "end_date" )  )
        self.write( json.dumps( {  "ret" : ret , "data" :  data } ) )

class ChannelSummary( CrossDomainHandler ):
    def get( self  ,channel_id ,start ,end  ):
        try :
            times = int( self.get_argument( "times" ,  -1  )  )
            res  = summary_get( channel_id , float( start  ) ,float( end  ) , times )
            self.write( json.dumps( {  "ret" : 0 , "data" : res, "count" : len(res ) } ) )
        except Exception as err :
            res  = summary_get( channel_id , float( start  ) ,float( end  )  )
            self.write( json.dumps( {  "ret" : 0 , "data" : res, "count" : len(res ) } ) )

class AudioSourceWatch( LocalRequestHandler ):
    def post(self):
        _id = int( self.get_argument( "id" ,  -1  )  )
        _status = int( self.get_argument( "status" ,  -1  )  )
        mark_audio_notify_rsp( _id , _status )
        self.write( json.dumps( { "ret" : 0 } ) )

class AudioSampleCheck( CrossDomainHandler ):
    def get( self  ,channel_id ,source_ch ,source_id  ):
        self.write( audio_mark_recheck( channel_id , source_ch ,source_id ) )

define("port", default=8004, help="run on the given port", type=int)

if __name__ == "__main__":
    tornado.options.parse_command_line()

    app = tornado.web.Application(
    [
        (r"/day_rand", DayRandHandler),
        (r"/producing_channel_list", ProductingChList),
        (r"/collecing_stream_info", ChannlCollectSourceList),
        (r"/channel_detail_report.csv", ChannDetailRep),
        (r"/channel_processing_detail", ChannelProcessDetail) ,
        (r'/channel/(?P<channel_id>.*)/stream_info', mk_req_handler( get = lambda self , channel_id : self.write( json.dumps( stream_info_get( channel_id )  ) ) ) ) ,
#        (r'/stream_seg_switch/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', StreamSegSwitch)  ,
        (r'/vs_playback.m3u8', VStreamPlayBack)  ,
        (r'/vs_playback', VStreamPlayBack)  ,
        (r'/stream_switch', StreamSwitch)  ,
        (r'/stream_attach', StreamAttach)  ,
        (r'/free_streams', mk_req_handler2( get =  lambda self : self.write( json.dumps( {  "ret" : 0 , "data" :  free_stream_get() } ) ) ) )  ,
        (r'/err_collect_notify', mk_req_handler( get = lambda self :  ret_write( self , 0 , data =  err_collet_ch_list() ) ))  ,
        (r'/crossdomain.xml', CrossDomainHandler),
        (r'/update_advice_for_bad_sample', BadSampleAdviceAdd  ) ,
        (r'/channel_produce_add', ChannelProduceAdd  ) , 
        (r'/channel_produce_modify', ChannelProduceModify  ) ,
        (r'/empty_task_insert', EmptyTaskInsert  ) ,
        (r'/edit_sys_check', mk_req_handler( get = edit_sys_check )  ) ,
        (r'/collect_sumary', CollectSummary  ) ,
        (r'/vs_gap_summary/(?P<channel_id>.*)/(?P<s_date>.*)/(?P<e_date>.*)/', mk_req_handler( get = lambda self, channel_id , s_date , e_date : self.write( json.dumps( {  "ret" : 0 , "data" :  vs_gap_summary(  channel_id , s_date, e_date ) } ) )  ) )  ,
        (r'/channel_produce_status', ChannelProduceStatus  ) ,
        (r'/cloud_cut_status', ChannelCloudCutStatus  ) ,
        (r'/cloud_produce_status', mk_req_handler( post =  cloud_produce_status ) ) ,
        (r'/summary/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', ChannelSummary)  ,
        (r'/audio_watch_rsp', AudioSourceWatch)  ,
        (r'/stream_update_time/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', StreamUpdateTime)  ,
        (r'/stream_source/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', mk_req_handler( get = lambda self , channel_id , start ,end :  ret_write( self , 0 ,  data =  stream_source_detail( channel_id ,  int( start  ), int( end  ))  ) ) ) ,
        (r'/produce_status/(?P<channel_id>.*)/(?P<date>.*)/', mk_req_handler( get =  produce_status ) ) ,
        (r'/stream_channel/(?P<channel_outer_id>.*)/(?P<source_id>.*)/', mk_req_handler( get =  stream_channel ) ) ,
        (r'/tmp_check', mk_req_handler( get = lambda x :  x.write( not_done_channel_list() )  )  ) ,
        (r'/cloud_produce_efficent', mk_req_handler( post = cloud_produce_efficent ) ) ,
        (r'/undone_edit_task_count', mk_req_handler( post = undone_edit_task_count ) ) ,
        (r'/unchecked_sp_count', mk_req_handler( post = unchecked_sp_count ) ) ,
        (r'/sample_to_word/(?P<sp_uuid>.*)/', mk_req_handler( get = sample_to_word  )), 
        #(r'/add_sample', mk_req_handler( post = sample_info_add  ) ) ,
        (r'/bad_summary/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', mk_req_handler( get = lambda self , channel_id , start ,end  : ret_write( self , 0 ,   data = bad_summary_list( channel_id , int( start  ), int(  end  ))  ) ) ) , 
        (r'/view_edit_tip/(?P<channel_id>.*)/(?P<start>.*)/(?P<end>.*)/', mk_req_handler( get = lambda self , channel_id , start , end  : ret_write( self , 0 ,   data = get_tip_data( channel_id , int( start  ), int(  end  ))  ) ) ) , 
        (r'/channel_id_from_outer_to_innerhannel_id/(?P<channel_outer_id>.*)/(?P<source_id>.*)/', mk_req_handler( get =  channel_id_from_outer_to_inner_call ) ) , 
        (r'/channel_id_from_outer_to_inner_multi/', mk_req_handler( get =  channel_id_from_outer_to_multi ) ) , 
          
        ] + 
        err_check.handler_list  + 
        stream_service.handler_list +
        new_produce.handler_list+ 
    new_produce_for_fast.handler_list  +
    bro_mark_check.handler_list  +
#        epg.handler_list+ 
        removed_sp_process.handler_list )

    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()
