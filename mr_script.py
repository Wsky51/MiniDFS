import struct
import math

import numpy as np
import sys
import os
import pandas as pd


# #传入一个文件，计算大小，均值和方差
# def test_mean_var_by_np(local_path):
#     data = np.loadtxt(local_path, dtype=int);
#     return len(data),data.mean(),data.var()

# 传入一个文件，计算大小，均值和方差
def mapper(local_path):
    data = np.loadtxt(local_path, dtype=int)
    return "{},{},{}".format(len(data),data.mean(),data.var())


#默认传过来的是字符串
def reducer(param1,param2):
    res1=[0,0,0]
    res2=[0,0,0]
    if isinstance(param1,str):
        temp = param1.strip().split(",")
        res1=[int(temp[0]),float(temp[1]),float(temp[2])]
    if isinstance(param2,str):
        temp = param2.strip().split(",")
        res2 = [int(temp[0]), float(temp[1]), float(temp[2])]

    N1=res1[0]
    N2=res2[0]

    M1=res1[1]
    M2=res2[1]

    mean=(res1[0]*res1[1]+res2[0]*res2[1])/(res1[0]+res2[0])

    SD1=res1[2]
    SD2=res2[2]

    part0=(N1-1)*math.pow(SD1,2)+(N2-1)*math.pow(SD2,2)
    part1=(N1*N2/(N1+N2))*(math.pow(M1,2)+math.pow(M2,2)-2*M1*M2)
    frac1=part0+part1
    frac2=N1+N2-1

    N=N1+N2
    M=mean
    VAR = math.sqrt(frac1 / frac2)

    return "{},{},{}".format(N,M,VAR)

argv = sys.argv
argc = len(argv) - 1
cmd = argv[1]

if cmd == '-mapper':
    if argc == 2:
        path = argv[2]
        print(mapper(path))
    else:
        print("Usage: python this_script.py -mappper <path>")

elif cmd == '-reducer':
    if argc == 3:
        param1 = argv[2]
        param2 = argv[3]
        print(reducer(param1,param2))
    else:
        print("Usage: python this_script.py -reducer <res1> <res2>")
else:
    print("undefined cmd,you can only run -mapper or -reducer")
    