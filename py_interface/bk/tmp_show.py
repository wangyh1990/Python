from tools.network import * 
from tools.time_convert import * 

tmp_ch_list  = ( '11000010008521' , 
'11000010007965' , 
'11000000004692' , 
'11000000004694' , 
'11000000004695' , 
'11000010006986' , 
'11000000004709' , 
'11000010002369' , 
'11000010002368' , 
'11000010002378' , 
'11000010002377' , 
'11000010002370' , 
'11000010002371' , 
'11000010002379' , 
'11000000004708' , 
'11000010002380' , 
'11000010002391' , 
'11000000004696' , 
'11000010004277' , 
'11000010002832' , 
'11000010002714' , 
'11000010002955' , 
'11000010002754' , 
'11000010002364' , 
'11000000014694' , 
'11000010004113' , 
'11000010004129' , 
'11000010003700' , 
'11000010003740' , 
'11000010003744' , 
'11000010003736' , 
'11000010003709' , 
'11000010002381' , 
'11000000014692' , 
'11000010002374' , 
'11000010002375' , 
'11000010002751' , 
'11000010003711' , 
'11000010002383' , 
'11000010002849' , 
'11000010002384' , 
'11000010002372' , 
'11000010002787' , 
'11000010002941' , 
'11000010002848' , 
'11000010004091' , 
'11000010002825' , 
'11000010002365' , 
'11000010002894' , 
'11000010002983' , 
'11000010002780' , 
'11000010002362' , 
'11000010003002' , 
'11000010002737' , 
'11000010002376' , 
'11000010003401' , 
'11000010005965' , 
'11000010006817' , 
'11000010002996' , 
'11000010005785' , 
'11000010006845' ) 

def not_done_channel_list():
    global tmp_ch_list
    tmp  =  [  [ i[ 0 ] ,  ts_to_time_str( i[ -3 ] )  , "[%s]" % i[ -1 ] ,i[ 1 ]   ] for i in  json.loads( url_req( "http://116.62.79.116/repeat.json" ,timeout_sec = 1 , try_count = 2 ) )[ "data" ]  ]
    tmp = sorted(   filter( lambda x : x[ 0 ]  in tmp_ch_list , tmp ) , key = lambda x : x [ 1 ] ) 
    return "\n".join( [   " " .join ( [ str( x ) for x in i  ]  )  + "<br>"  for i in tmp ] )

