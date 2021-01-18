import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options
from local_utils import * 


from tools import db
import json
import os,sys

def list_to_dict( list_data , hash_fc ):
    return { hash_fc( x ) :x for x in list_data  } 

def combine_mp3_v1( self , channel_id , day ):
    res =  db.query( 'select ind ,  url from combine_mp3_v1 where channel  =  "%s" and day = "%s" and media_type = 0 and url like "http%%"   '  %( channel_id , day)  , 'db_fast' ) 
    res2 = list_to_dict( res , lambda x : int(  x[ 'ind' ])   )
    data = []
    for x in range( 1 , 7  ) :
        tmp_dic = {}
        tmp_dic['url' ] = res2.get(  x , {} ).get( 'url' , '' )
        tmp_dic['json' ] = tmp_dic['url' ].replace( '.mp3'  ,'.json' )
        data.append( tmp_dic )
    ret_write( self , 0 ,  data )

handler_list = [
    (r'/fast/combine_mp3_v1/(?P<channel_id>.*)/(?P<day>.*)/', mk_req_handler( get = combine_mp3_v1  ) ) ,
]

define("port", default=8014, help="run on the given port", type=int)

if __name__ == "__main__":
    tornado.options.parse_command_line()

    app = tornado.web.Application( handler_list )

    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.current().start()
