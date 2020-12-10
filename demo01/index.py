#!/usr/bin/python3
import os
print('Process (%s) start...' % os.getpid())
# Only works on Unix/Linux/Mac:
pid = os.fork()
if pid == 0:
    print('I am child process (%s) and my parent is %s.' % (os.getpid(), os.getppid()))
else:
    print('I (%s) just created a child process (%s).' % (os.getpid(), pid))
# # if __name__ == '__main__':
#     # print('命令行参数如下:')
#     # for i in sys.argv:
#     #     print(i)
#     # print('\n')
#     # languages = ["C", "C++", "Perl", "Python"]
#     # index = languages.index('Python')
#     # print(index)
#     # for x in languages:
#     #     print(x,end=':')
#     #     print(languages.index(x),end=';;')
#
#     # var = 1
#     # while var == 1:  # 表达式永远为 true
#     #     num = int(input("输入一个数字  :"))
#     #     print("你输入的数字是: ", num)
#
#     # print("Good bye!")
#
#     # Fibonacci series: 斐波纳契数列
#     # 两个元素的总和确定了下一个数
#     a, b = 0, 1
#     while b < 1000:
#         print(b,end='~')
#         a, b = b, a + b
#     print('\n')
#     tup1 = ('Google', 'Runoob', 1997, 2000)
#     tup2 = (1, 2, 3, 4, 5, 6, 7)
#
#     print("tup1[0]: ", tup1[0])
#     print("tup2[1:5]: ", tup2[1:5])
#
#     print('Process (%s) start...' % os.getpid())
#     # Only works on Unix/Linux/Mac:
#     pid = os.fork()
#     if pid == 0:
#         print('I am child process (%s) and my parent is %s.' % (os.getpid(), os.getppid()))
#     else:
#         print('I (%s) just created a child process (%s).' % (os.getpid(), pid))