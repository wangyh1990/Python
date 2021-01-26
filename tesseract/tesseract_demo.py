import time
from PIL import Image
from pytesseract import *

stime = time.time()
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
im = Image.open('/Users/wangyuhou/Downloads/1.jpeg')
text = image_to_string(im, lang='chi_sim')
with open('/Users/wangyuhou/Downloads/result_'+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+'.txt', 'w') as f:
    f.write(text)
print(text)
print(round(time.time()-stime, 2))
