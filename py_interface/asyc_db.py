import asyncio
import tormysql
import pymysql
from tornado import gen
import configparser
from functools import wraps

_cfg = configparser.ConfigParser()
_cfg.read( "config.ini" )

_asyc_sql_pool = {}

def get_cursor_class( config_type ):
    if "dict" == config_type:
        return pymysql.cursors.DictCursor
    else:
        return pymysql.cursors.Cursor

def get_db_ins( db_name ):
    global _asyc_sql_pool
    if db_name in _asyc_sql_pool : return _asyc_sql_pool[ db_name ]

    for i in [ 'host',  'db' , 'user' , 'password' , 'port']:
        if "what?" ==  _cfg.get( db_name , i , fallback = "what?" ) :
            err =  " no %s for %s in %s " %(  i , db_name ,tools.config.config_filename )
#            get_logger().error( err )
            raise Exception( err )
    _asyc_sql_pool[ db_name ] =  tormysql.ConnectionPool(
                max_connections = 20, #max open connections
                idle_seconds = 500, #conntion idle timeout time, 0 is not timeout
                wait_connection_timeout = _cfg.getfloat( db_name , "connect_sleep_sec" , fallback = 3 ) ,
                host = _cfg.get(db_name, "host"),
                db=_cfg.get(db_name, "db"),
                user=_cfg.get(db_name, "user"),
                passwd=_cfg.get(db_name, "password"),
                cursorclass =  get_cursor_class( _cfg.get(db_name, "cursor" ,fallback = "" ) ) ,
                charset = "utf8" )
    return _asyc_sql_pool[ db_name ]

def sql_log( func ):
    @wraps( func )
    def wrapper( *args , **kwargs ):
#        get_logger().debug( "sql : %s" % args[ 0 ] ) 
        if type( args[ -1 ] ) == str:
            return func(*args, **kwargs)
        elif type( args[ -1 ] ) == list :
            ret = []
            new_args = list( args )
            for i in args[ -1 ]:
                new_args[ -1 ] = i
                print( new_args )
                ret.append( func(*new_args, **kwargs) ) 
            return ret
    return wrapper

@sql_log
@gen.coroutine
def query(sql , db_name = "db" ) :
    try :
        _pool= get_db_ins( db_name )
        with ( yield _pool.Connection() ) as conn:
            with conn.cursor() as cursor:
                yield  cursor.execute( sql )
                ret = cursor.fetchall()
                return ret
    except Exception as err:
        print( err )

@sql_log
@gen.coroutine
def execute(sql , db_name = "db" ) :
    try :
        _pool= get_db_ins( db_name )
        with ( yield _pool.Connection() ) as conn:
            with conn.cursor() as cursor:
                yield  cursor.execute( sql )
    except Exception as err:
        print( err )
