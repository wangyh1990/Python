#coding=utf8

import io
import wave

import matplotlib.pyplot as plt  # 专业绘图库
import numpy as np
from pydub import AudioSegment
from scipy.io import wavfile

# 先从本地获取 mp3 的 bytestring 作为数据样本
filename = "/Users/wangyuhou/Downloads/28D614E35DF04B47A2A8E7AECA6A5919.mp3"
fp = open(filename, 'rb')
data = fp.read()
fp.close()
# 读取
aud = io.BytesIO(data)
sound = AudioSegment.from_file(aud, format='mp3')
raw_data = sound._data

# 写入到文件
l = len(raw_data)
f = wave.open(filename + ".wav", 'wb')
f.setnchannels(1)
f.setsampwidth(2)
f.setframerate(16000)
f.setnframes(l)
f.writeframes(raw_data)
f.close()

# 读取生成波形图
samplerate, data = wavfile.read(filename + ".wav")
times = np.arange(len(data))/float(samplerate)
# print(len(data), samplerate, times)

# 可以以寸为单位自定义宽高 frameon=False 为关闭边框

fig = plt.figure(figsize=(20, 5), facecolor="white")
# plt.tick_params(top='off', bottom='off', left='off', right='off', labelleft='off', labelbottom='on')

ax = fig.add_axes([0, 0, 1, 1])
ax.axis('off')
plt.fill_between(times, data, linewidth = '1', color='green')
plt.xticks([])
plt.yticks([])
plt.savefig(filename + '.png', dpi=100, transparent=False, bbox_inches='tight', edgecolor='w')
plt.show()
