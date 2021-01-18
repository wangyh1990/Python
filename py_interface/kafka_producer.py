import time
import json
from kafka import KafkaProducer
import threading
from tools.decorates import * 
# Assign a topic

class CKafkaProducer:
    def __init__( self,  bootstrap_servers  ):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.lock  = threading.Lock()
        self.last_coneect = 0

    def connect( self ) :
        if time.time() - self.last_coneect < 5.0 :
            get_logger().error('too close shall not re connect ')
            return False
        try:
            self.producer = KafkaProducer(bootstrap_servers =  self.bootstrap_servers ,  max_block_ms = 222 )
            self.last_coneect = time.time()
            return True
        except Exception as e:
            get_logger().error(" %s reconnect error" % e)
            self.producer = None
            self.last_coneect = time.time()
            return False

    @by_thread
    def send( self ,topic,  info  ):
        get_logger().info(" %s  %s send " %  ( topic , info ))
        self.lock.acquire()
        try:
            if None == self.producer :
                self.connect()

            if None == self.producer  : 
                self.lock.release()
                return False
            if type( info ) is not str :
                info = json.dumps( info )
            ret = self.producer.send(topic, str(info).encode() )
            print(  ret )
            self.lock.release()
            return True
        except Exception as e:
            get_logger().error(" %s is error" % e)
            self.lock.release()
            return False

    def close( self ):
        self.close()

G_center_kk = CKafkaProducer( [ '192.168.0.152:9092' ] )
