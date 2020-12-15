from common import *
import os
import struct
import time
from enum import Enum
import threading

class CacheSwapoutStrategy(Enum):
    #采用最近最久未使用策略
    LRU=1
    #采用最近最少使用策略
    LFU=2

#获取不同节点上的pid号
def getPid(node):
    cmd = "ssh {} \'netstat -apn | grep 14269\'".format(node)
    back_info = os.popen(cmd).read()

    #如果该端口存在说明服务已经启动了，返回pid号
    if (back_info.find("14269")>0 and back_info.find("/python")>0):
        idx = back_info.find("/python")
        return back_info[idx - 5:idx].strip()
    #否则返回Not alive
    else:
        return "Not alive"
    
# 遍历所有文件夹下的文件
def walk_files(path):
    res=[]
    for root, dirs, files in os.walk(path, topdown=False):
        root="/"+root[len(path):]
        for name in files:
            res.append(root+"/"+name)
    return res

def comparedata(a,b):
    return float(a["mem_prop"][:-1])-float(b["mem_prop"][:-1])


#写文件线程
class StroeThread(threading.Thread):
    global sem
    def __init__(self, local_path, chunk_data,semaphore=None):
        super().__init__(name=local_path)
        self.local_path = local_path
        self.chunk_data = chunk_data
        self.sem=semaphore

    def run(self):
        if self.sem is not None:
            self.sem.acquire()
        with open(self.local_path, "wb") as f:
            f.write(self.chunk_data)
            print("写入本地磁盘成功，块为：",self.local_path)
            if self.sem is not None:
                self.sem.release()

# 注意，此处的data已经默认是经过utf-8编码的
def strong_sck_send(socket, data):
    global PACK_SIZE
    global PACK_SIZE_FMT
    size = len(data)
    idx = 0;

    # 先告知对端目前要发送的数据量大小是多少
    socket.send(struct.pack('L', size))
    while idx < size - PACK_SIZE:
        unpack = struct.unpack_from(PACK_SIZE_FMT, data, idx)[0]
        idx += PACK_SIZE
        # 依次发送数据
        socket.sendall(unpack)
    # 解析最后的不到PACK_SIZE的数据并发送
    last = size - idx
    unpack = struct.unpack_from(str(last) + 's', data, idx)[0]
    socket.sendall(unpack)


# 返回未经编码的比特流
def strong_sck_recv(socket):
    global PACK_SIZE
    data_info_size = struct.calcsize('L')
    # 接收大小信息
    buf = socket.recv(data_info_size)
    # while buf==b'':
    #     time.sleep(0.1)
    #     buf = socket.recv(data_info_size)

    # 接收端接受数据大小信息
    data_size = struct.unpack('L', buf)[0]
    recvd_size = 0  # 定义已接收文件的大小

    res = b''
    while not recvd_size == data_size:
        if data_size - recvd_size > PACK_SIZE:
            data = socket.recv(PACK_SIZE)
            recvd_size += len(data)
            res += data
        else:
            data = socket.recv(data_size - recvd_size)
            recvd_size = data_size
            res += data
    return res


def str_encode_utf8(strdata):
    return bytes(strdata, encoding='utf-8')


def utf8_decode_str(data):
    return str(data, encoding='utf-8')



