from PIL import Image
from Pytesseract import *

im = Image.open('/Users/wangyuhou/Downloads/Xnip2021-01-26_06-51-11.jpg')
text = image_to_string(im,lang='chi_sim')

print(text)