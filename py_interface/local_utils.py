from tools.time_convert  import * 
from tools import db
from  tornado.web import access_log
from  tools.decorates import *
from  tools.network import *
from  functools  import wraps
from  tools.log_factory import *
import traceback 
import datetime
import tornado.web
import random
import threading
import json
import requests

def get_start_ts_and_day_count( dt ):
    dt = dt + "-01 00:00:00"
    next_mon_str =  ts_to_time_str( time_str_to_ts( dt ) +  86400 * 34 )[ 0:7] + "-01 00:00:00" 
    return  time_str_to_ts( dt ) ,  int( ( time_str_to_ts( next_mon_str ) - time_str_to_ts( dt ) + 1.0  ) / 86400 )

def _random_list_make( m , n , chosen_list ) :
    choice_count = m
    month_day_count =  n
    if choice_count > month_day_count : return  "[]"
    day_left = list( range(1 , month_day_count + 1 )  ) 
    result = set( [ i - 1  for i in chosen_list ]  )
    tmp = [[ x for x in range( i , month_day_count   , 7   ) ] for i in range( 7 ) ]
    row_count = [ 0,0,0,0,0,0 ]
    col_count = [ 0,0,0,0,0,0 ,0 , 0 ]
    for i in range ( n ) :
        row_count[ i // 7 ] = row_count[ i //  7 ] + 1 
        col_count[ i % 7 ] = col_count[ i % 7 ] + 1 

    checked_row = [ i // 7 for i in result ]
    checked_col = [ i % 7 for i in result ]

    for i in result : 
        col_count[ i %7 ] =  col_count[ i %7 ] - 1
        row_count[ i // 7 ] =  row_count[ i // 7 ] - 1

#    #print( result )
    while len( result )!= choice_count :
        cur_samples = []
        #print( checked_col )
        for x in range( 7 ) :
            if x in checked_col : continue
            for y in range( col_count[ x ] ) :
                cur_samples.append( x )

        if cur_samples ==  []:
            checked_col =  []
            for x in range( 7 ) :
                for y in range( col_count[ x ] ) :
                    cur_samples.append( x )
        col = random.sample( cur_samples  , 1 )[ 0 ]
        checked_col.append( col )
        #print( "col " ,col )

        cur_samples = []
        for row in range( ( n -1 ) // 7 + 1  ):
            #print( row, "row " )
            k =  row * 7 + col
            if k not in result and k < n  and row not in checked_row :
                for x in range( row_count[ row ] ):
                    cur_samples.append( k  )

        if [] == cur_samples :
            checked_row = []

            for row in range( ( n -1 ) // 7 + 1  ):
                k =  row * 7 + col
                if k not in result and k < n  :
                    for x in range( row_count[ row ] ):
                        cur_samples.append( k  )

        if [] == cur_samples :
            #print( "reset" )
            checked_col = []
            cur_samples = list(   set( list(  range( n ) )  ) - result  )

        #print( cur_samples ,"cur_samples" )

        res  = random.sample( cur_samples , 1 )[ 0 ]
        result.add( res )
        row_count[ res // 7  ] =  row_count[ res // 7  ] - 1
        checked_row.append( res // 7 )

    return  [ i + 1  for i in sorted( result  ) ] 
#    return json.dumps( [ i + 1  for i in sorted( result  ) ] ) 

from tools.self_iter import *

def get_list_continuous_count( data ):
    count = 1 
    max_count = 0 
    for i , j  in list_data( data ,2 ) : 
        if i + 1 == j :
            count = count + 1 
        else: 
            max_count = max( max_count , count )
            count = 1

    return max( max_count , count )

def random_list_make( m , n , chosen_list ):
    res = _random_list_make(  m , n  , chosen_list )
    if n == m : return json.dumps( list( range( 1, n + 1  ) ) )
    _count_allowed =   int(m / ( n - m  ) ) + 1
    for i in range( 20 ) : 
        if get_list_continuous_count( res ) > _count_allowed  :
#            print( "try" )
            res = _random_list_make( m , n , chosen_list )
        else: 
            break 
    return  json.dumps( res  )

#____________________________ 
class LocalRequestHandler( tornado.web.RequestHandler ) :
    def write_error(self, status_code, **kwargs):
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

        log_method("%d %s %s (%s) %s %s %.2fms%s",
                   self.get_status(), self.request.method,
                   self.request.uri, self.request.remote_ip,
                   self.request.headers.get(  "User-Agent" , ""),
                   self.request.arguments,
                   request_time , err_msg)

class CrossDomainHandler( LocalRequestHandler ) :
    def prepare( self ):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('X-Content-Type-Options', 'nosniff')

class CrossDomainHandler3( LocalRequestHandler ) :
    def prepare( self ):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('X-Content-Type-Options', 'nosniff')

class CrossDomainHandler2( LocalRequestHandler ) :
    def prepare( self ):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Credentials', 'true')

import uuid 
def mk_req_handler( get = None , post = None , is_cross = True ):
    fc = {}
    if None != get :  fc[ "get" ] = get 
    if None != post :  fc[ "post" ] = post 
    cls = type( str( uuid.uuid1() )  , ( CrossDomainHandler  ,  ) , fc) 
    return cls

def mk_req_handler2( get = None , post = None , is_cross = True ):
    fc = {}
    if None != get :  fc[ "get" ] = get 
    if None != post :  fc[ "post" ] = post 
    cls = type( str( uuid.uuid1() )  , ( CrossDomainHandler2  ,  ) , fc) 
    return cls

def mk_req_handler3( get = None , post = None , is_cross = True ):
    fc = {}
    if None != get :  fc[ "get" ] = get 
    if None != post :  fc[ "post" ] = post 
    cls = type( str( uuid.uuid1() )  , ( CrossDomainHandler3  ,  ) , fc) 
    return cls

def handler( arg0 ):
    def wrapper( arg1 ):
        @wraps( arg1 )
        def foo2( *args , ** kwargs ):
            return arg1( *args, ** kwargs ) 
        return type( str( uuid.uuid1() )  , ( CrossDomainHandler2  ,  ) , { arg0 :  foo2 } ) 

    @wraps( arg0 )
    def foo3( *args , ** kwargs ):
        return arg0( *args, ** kwargs ) 

    if callable( arg0 ) :
        return type( str( uuid.uuid1() )  , ( CrossDomainHandler2  ,  ) , { 'get' :  foo3 } )  
    return wrapper

def para_check( para_list ):
    def do_check(func):
        def foo(*args, **kwargs):
            for x in para_list:
                if args[ 0 ].get_argument( x  , '!@!#!##!sdfasfasfasjf' ) == '!@!#!##!sdfasfasfasjf'  :
                    ret_write( args[ 0 ] , -1 , '参数不足' )
                    return 
            return func(*args, **kwargs)
        return foo
    return do_check

def get_search_range( self  ,p_kw = 'page', l_kw = 'limit' ):
    try :
        pg = int(  self.get_argument( p_kw  , "1")  )
        limit =  int( self.get_argument(  l_kw  , "1000000")  )
        return  limit * ( pg - 1  ) , limit * ( pg )
    except :
        return  0, 11111111

def ret_write( self , ret = 0  , data = None  ):
    if None != data :
        self.write( json.dumps( {  "ret" : ret , "data" :  data } ) )
    else:
        self.write( json.dumps( {  "ret" : ret } ) )

def handler_rsp( arg0 ):
    def _main( fc ,  self, args, kwargs  ):
        ret = fc( * [ kwargs[ x ] for x in  fc.__code__.co_varnames[ : fc.__code__.co_argcount ] ]  )
        ret_write( self ,  * ret  )
    def _handler_mk( kw ):
        return  type( str( uuid.uuid1() )  , ( CrossDomainHandler2  ,  )  , kw  ) 

    def foo_( self, * args , **kwargs ):
        _main( arg0 , self , args , kwargs )

    def _foo( arg1 ):
        def foo__( self, * args , **kwargs ):
            _main( arg1 , self , args , kwargs )
        return  _handler_mk( { arg0:foo__ } )

    if callable( arg0 )  :
        return  _handler_mk( { 'get' : foo_ } ) 
    else :
        return  _foo

#____________________________ 

def dict_list_update(  l , d , func = None   ):
    [ i.update( d )  for i in l if None == func or func( i ) ]
    return l

def get_channel_name_map( ch_list ):
    if  len( ch_list )  < 2 : 
        ch_list = ch_list +  [ "java" , "java2" ]
    ch_list = "channel=" +  json.dumps( ch_list )
    res  =  js_load_req( "http://47.96.182.117/manage/index/getChannelName" , post_str = ch_list ,timeout_sec = 1 , try_count = 2 )
    res = res[ "data" ]
    return res

def _group_by( data, key_func = None ):
    ret = {}
    key_func = key_func or (  lambda x:x )
    for x in data  :
        datakey =   key_func( x )
        if datakey not in ret :
            ret[ datakey ] =  [ x ]
        else:
            ret[ datakey ].append(  x  )
    return ret

def seg_join_short_short( s1 ,t1 ,s2 ,t2  ):
    if s1 >= t1 or s2 >=t2 or t1 <= s2 or t2 <= s1  : return False
    d = min( t1 ,t2 ) - max( s1 , s2 )  + 0.0
    return d > 3.0 or  d / ( t1 - s1 )  > 0.3 or d /  ( t2 - s2 ) > 0.3

def seg_join_long_short( s1 ,t1 ,s2 ,t2  ):
    dura1, dura2 = t1 -s1 , t2-s2
    d = seg_join_length( s1,t1,s2,t2 )
    return d / max(  dura1 , dura2 ) ,  d / min( dura1 , dura2 )

def common_summary_exist( ch , start , end ):
    start ,end = float( start ) , float( end )
    if  start  >= end  : return True , 0 ,  0
    data_range = 1800
    sql_cmd =  "select start ,end from summary where channel = '%s' and ( start between %d and %d )  and ( end  between %d  and %d   ) and tag in ( 0 ,1  ) and duration < 300  "%  ( ch ,  start - data_range , end + data_range  , start - data_range , end +  data_range )
    res_list = db.query( sql_cmd ) 
    advice_start , advice_end =  start , end
    for i in res_list:
        if seg_join_short_short( start , end , i[ "start" ] , i[ "end" ] ):
            return True ,  0 , 0
        else :
            if i[ "start" ] < advice_start  and  advice_start < i[ "end" ] :
                advice_start = i[ "end" ]
            if i[ "start" ] < advice_end  and advice_end   < i[ "end" ] :
                advice_end = i[ "start" ]
    data_range = 3800
    sql_cmd =  "select start ,end from summary where channel = '%s' and ( start between %d and %d )  and ( end  between %d  and %d   ) and tag in ( 0 ,1  ) and duration >= 300  "%  ( ch ,  start - data_range , end + data_range  , start - data_range , end +  data_range )
    res_list = db.query( sql_cmd ) 
    for i in res_list:
        join_perc =  seg_join_long_short( advice_start , advice_end , i[ "start" ] , i[ "end" ] )
        if  min( join_perc ) > 0.2 :# min join perc
            return True ,  0 , 0
        else :
            if max( join_perc ) > 0.5  : #max join perc ,just insert , do not adjust
                continue
            if i[ "start" ] < advice_start  and  advice_start < i[ "end" ] :
                advice_start = i[ "end" ]
            if i[ "start" ] < advice_end  and advice_end   < i[ "end" ] :
                advice_end = i[ "start" ]
    if ( advice_end - advice_start )  / ( end - start ) < 0.5 :
        return True , 0 , 0 
    return False , advice_start , advice_end

def common_summary_exist1( ch , start , end ):
    start ,end = float( start ) , float( end )
    if  start  >= end  : return True , 0 ,  0
    data_range = 1800
    sql_cmd =  "select start ,end from summary where channel = '%s' and ( start between %d and %d )  and ( end  between %d  and %d   ) and tag in ( 0 ,1  ) and duration < 300  "%  ( ch ,  start - data_range , end + data_range  , start - data_range , end +  data_range )
    res_list = db.query( sql_cmd ) 
    advice_start , advice_end =  start , end
    for i in res_list:
        if seg_join_short_short( start , end , i[ "start" ] , i[ "end" ] ):
            return True ,  0 , 0
        else :
            if i[ "start" ] < advice_start  and  advice_start < i[ "end" ] :
                advice_start = i[ "end" ]
            if i[ "start" ] < advice_end  and advice_end   < i[ "end" ] :
                advice_end = i[ "start" ]
    data_range = 3800
    sql_cmd =  "select start ,end from summary where channel = '%s' and ( start between %d and %d )  and ( end  between %d  and %d   ) and tag in ( 0 ,1  ) and duration >= 300  "%  ( ch ,  start - data_range , end + data_range  , start - data_range , end +  data_range )
    res_list = db.query( sql_cmd ) 
    for i in res_list:
        join_perc =  seg_join_long_short( advice_start , advice_end , i[ "start" ] , i[ "end" ] )
        if  min( join_perc ) > 0.2 :# min join perc
            return True ,  0 , 0
        else :
            if max( join_perc ) > 0.5  : #max join perc ,just insert , do not adjust
                continue
            if i[ "start" ] < advice_start  and  advice_start < i[ "end" ] :
                advice_start = i[ "end" ]
            if i[ "start" ] < advice_end  and advice_end   < i[ "end" ] :
                advice_end = i[ "start" ]
    if ( advice_end - advice_start )  / ( end - start ) < 0.5 :
        return True , 0 , 0 
#    print( ( advice_end - advice_start ) / ( end - start )  )
    return False , advice_start , advice_end

from  tools.sql_mk  import *
from  tools.math  import *

def _err_ansi_range( channel_id , start , end ): 
    start, end = int( start) ,int( end )
    end = min( end ,start + 86400 )
    res = db.query( "select start ,end ,task_uuid from task where channel = '%s' and start between %s and %s and end  between %s and %s and status in( 2,92 )  order by start " %( channel_id , start - 4000 , end , start ,end  + 4000   ) , "db" )
    if len( res ) != 0  :
        res2 = db.query( "select task_uuid  from ad.repeat where task_uuid %s and status = 9  " %( where_in( [ x[ 'task_uuid' ] for x in res ] ) ) , "db" )
        err_task_set =  [ x[ "task_uuid" ] for x in res2 ]
    else:
        err_task_set = []
    res =  list( filter(  lambda x  :x [ 'task_uuid' ] not  in err_task_set  and seg_join_length( x[ "start" ] , x[ "end" ] ,start , end ) > 0  , res )) 
    res = range_list_combine( res , f ="start"  ,t= "end" )
    ret = [ [ start  , end ]]
    for x in res :
        tmp = []
        for y in ret : 
            tmp = tmp +  seg_complement( y[ 0 ] , y[ 1 ]  ,  x[ "start" ]  ,x[ "end" ])
        ret = tmp
    return ret


def get_ch_list_of_gp( gp ):
    if gp != -1 :
        return [ x[ "channel" ] for x in  db.query( "select channel from edit_lock where enable =1 and edit_lock.group  = %s  " %( gp ) ) ] 
    else:
        return [ x[ "channel" ] for x in  db.query( "select channel from edit_lock where enable =1  " ) ] 

def kw_set( kw ,val  ):
    db.query( "replace  into t_kw ( k , v ) values ( '%s' , %s )" %( kw , val ) , 'error_ansi' )

def kw_max( kw ,val  ):
    tmp = db.query( "select v from t_kw where k = '%s'" %( kw  ) , 'error_ansi' )
    if len( tmp) == 0  or tmp[ 0 ] [ 'v' ]  < int( val ) :
        kw_set( kw , val )

from tools.utils import summary_delete_notify
def summary_of_sp_delete( ch , sp_uuid , start_date ):
#    where_str = " where channel = '%s' and ad_uuid = '%s'"  % ( ch ,sp_uuid ) 
    where_str = " where channel = '%s' and ad_uuid = '%s' and start_time > '%s 00:00:00' "  % ( ch ,sp_uuid , start_date ) 
    res =  db.query( "select start from summary   %s " % where_str , 'db')
    [ summary_delete_notify( ch , x[ 'start' ]) for x in res ]
    res =  db.query( "update summary set tag =  17   %s " % where_str , 'db')

@try_catch( "" )
def ch_domain( ch ):
    return db.query( "select domain_name from edit_lock where channel = '%s' " %( ch ) , 'db' )[ 0 ][ 'domain_name' ]
 
def daemon_th_start( target = None , args = () ,kwargs = {} ):
    th = threading.Thread( target = target , args = args , kwargs =  kwargs ,daemon = True )
    th.start()

def by_thread(func):
    @wraps( func )
    def do_foo(*args, **kwargs):
        daemon_th_start( target = func , args = args ,kwargs = kwargs )
        return do_foo

def sp_process_notify( ch ):
    def foo():
        get_logger().info(  "sp_make notify %s %s " %( ch , js_load_req( 'http://%s:8022/%s/' %( ch_domain( ch ) , ch ) ,timeout_sec = 0.2 )[ 'ret' ]) ) 
    daemon_th_start( foo )

def or_( info ) :
    for x in info : 
        if x :
            return True
    return False

def and_( info ) :
    for x in info : 
        if not x :
            return False
    return True

def xor_( info ) :
    res = info[ 0 ]
    for x in info[ 1: ]: 
        res = res ^ x
    return res

def fc_test( * args1, **kwargs1 ):
    def test12( foo ):
        @wraps( foo )
        def foo2( *args , ** kwargs ):
            return foo( *args, ** kwargs ) 
        try :
            test_ret =  foo( * args1 , **kwargs1 )
            print( "%s  ( *%s ,  ** %s  )returns  %s " %( foo , args1 , kwargs1 , test_ret ) )
        except Exception as err  :
            print( "%s  ( %s , %s  )error now ~  %s  %s " %( foo , args1 , kwargs1 , err ,traceback.format_exc() ) )
    return test12

class FuncCallTimer:
    def __init__( self ):
        self.calls = {}
        self.lock  = threading.Lock() 
    def fc_recent_called(  self , fc , args ,kwargs ):
        call_key =  '%s_%s-%s' %( fc.__name__ , str(  args  ) ,str( kwargs ) ) 
        return self.recent_used( '%s_%s-%s' %( fc.__name__ , str(  args  ) ,str( kwargs ) ) )

    def recent_used(  self , kw ):
        self.lock.acquire()
        ret = time.time() - self.calls.get(  kw , 0 ) < 4
        if not  ret :
            self.calls[ kw ] = time.time()

        if len( self.calls ) > 30000 :
            self.calls = { x :y for x , y in self.calls.itmes()  if time.time() - y < 4  } 
        self.lock.release()
        return ret

G_func_call_timer = FuncCallTimer()

def double_call_forbid( err_ret  ):
    def foo1( foo  ):
        @wraps( foo )
        def foo2( *args , ** kwargs ):
            global G_func_call_timer 
            if G_func_call_timer.fc_recent_called( foo, args ,kwargs )  :
                return err_ret  
            else:
                return foo( *args, ** kwargs ) 
        return foo2
    return foo1

def get_search_range( self ):
    try :
        pg = int(  self.get_argument( "page"  , "1")  )
        limit =  int( self.get_argument( "limit"  , "1000000")  )
        return  limit * ( pg - 1  ) , limit * ( pg )
    except :
        return  0, 11111111
    
def get_channel_gp(channel, day):
    resp = js_load_req("http://47.96.182.117/index/getChannelDateTask?channel={}&date={}".format(channel, day) ,timeout_sec = 1 , try_count = 2)
    if resp["code"] == 0 and "group" in resp.keys():
        return resp["group"]
    else:
        return 99

def get_day_list(start, end, reverse=False):
    datestart = datetime.datetime.strptime(start, '%Y-%m-%d')
    dateend = datetime.datetime.strptime(end, '%Y-%m-%d')
    day_list = [str(datestart)[:10]]
    while datestart < dateend:
        datestart += datetime.timedelta(days=1)
        datestart.strftime('%Y-%m-%d')
        day_list.append(str(datestart)[:10])
    if reverse:
        day_list.reverse()
    return day_list


def parse_allcut(channel, start, end, err_count, source, auther, desc):
    try:
        allcut_url = 'http://b1.debug.hz-data.com/Allcut/Index/saveChannelBackInfo'
        allcut_source = {
            '0': 1,
            '1': 2,
            '2': 3
        }
        data = {
            "fmedia_id": channel,
            "start": start,
            "end": end,
            "err_num": err_count,
            "source": allcut_source.get(str(source), '99'),
            "check_user": auther,
            "desc": desc
        }
        resp = requests.post(allcut_url, json=data, timeout=10)
        assert json.loads(resp.content.decode())['code'] == 0, 'parse_allcut_error:{}'.format(resp.content.decode())
    except Exception as e:
        get_logger().error(str(e))
        pass
