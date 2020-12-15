import os
import socket
import sys
import threading
import time

import pandas as pd
from common import *
from utils import *
from queue import Queue

# DataNode支持的指令有:
# 1. load 加载数据块
# 2. store 保存数据块
# 3. rm 删除数据块
# 4. format 删除所有数据块
# 5. run_mapreduce执行mr操作

class DataNode:
    def run(self):
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", data_node_port))
            listen_fd.listen(5)
            listen_fd.setblocking(True)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("Received request from {}".format(addr))

                try:
                    # 获取请求方发送的指令
                    # request = str(sock_fd.recv(BUF_SIZE), encoding='utf-8')
                    request = utf8_decode_str(strong_sck_recv(sock_fd))
                    request = request.split()  # 指令之间使用空白符分割
                    print(request)

                    cmd = request[0]  # 指令第一个为指令类型

                    if cmd == "load":  # 加载数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.load(dfs_path)
                    elif cmd == "store":  # 存储数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.store(sock_fd, dfs_path)
                    elif cmd == "rm":  # 删除数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.rm(dfs_path)
                    elif cmd == "format":  # 格式化DFS
                        response = self.format()
                    elif cmd == "run_mapreduce":
                        file_name = request[1]
                        pyscript_path = request[2]
                        fat_path = request[3]
                        response = self.run_mapreduce(file_name, pyscript_path, fat_path)
                    else:
                        response = "Undefined command: " + " ".join(request)

                    # sock_fd.send(bytes(response, encoding='utf-8'))
                    strong_sck_send(sock_fd, str_encode_utf8(response))
                    print("dataname已经发送了数据")
                except KeyboardInterrupt:
                    break
                finally:
                    sock_fd.close()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(e)
        finally:
            listen_fd.close()

    def load(self, dfs_path):
        # 本地路径
        local_path = data_node_dir + dfs_path

        # #如果在缓存系统命中，则直接返回该数据
        if local_path in mem_data_map:
            mem_data_map[local_path][1]+=1
            mem_data_map[local_path][2]=time.time()
            print("缓存命中成功！,当前local_path：",local_path)
            return utf8_decode_str(mem_data_map[local_path][0])
        else :
            # 否则去磁盘读取本地数据
            with open(local_path) as f:
                chunk_data = f.read(dfs_blk_size)

        return chunk_data

    def store(self, sock_fd, dfs_path):
        global mem_count
        global data_node_mem_cache
        global hostname
        global sem
        global fifo_queue

        # 从Client获取块数据
        chunk_data = strong_sck_recv(sock_fd)
        # 本地路径
        local_path = data_node_dir + dfs_path
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))

        # 如果超过我们开始设定的最大缓存(即缓存用完)，则选择换出策略
        if mem_count > data_node_mem_cache*1024:
            #如果缓存策略采用的是LRU
            if default_cache_swapout_strategy == CacheSwapoutStrategy.LRU:
                sorted_mem_data = sorted(mem_data_map.items(), key=lambda item: item[1][2])
                #循环进行删除，直到给内存腾出足够的空间
                for i in range(len(sorted_mem_data)):
                    curr_data=sorted_mem_data[i]
                    curr_data_size=len(curr_data[1][0])
                    del mem_data_map[curr_data[0]]
                    mem_count-=curr_data_size
                    #如果新加进来的数据满足缓存大小的要求，那么现在就可以安全退出了
                    if mem_count+len(chunk_data)< data_node_mem_cache*1024:
                        break
            #如果缓存策略采用的是 LFU
            elif default_cache_swapout_strategy == CacheSwapoutStrategy.LFU:
                sorted_mem_data = sorted(mem_data_map.items(), key=lambda item: item[1][1])
                # 循环进行删除，直到给内存腾出足够的空间
                for i in range(len(sorted_mem_data)):
                    curr_data = sorted_mem_data[i]
                    curr_data_size = len(curr_data[1][0])
                    del mem_data_map[curr_data[0]]
                    mem_count -= curr_data_size
                    # 如果新加进来的数据满足缓存大小的要求，那么现在就可以安全退出了
                    if mem_count + len(chunk_data) < data_node_mem_cache * 1024:
                        break
            #如果缓存策略采用的是FIFO
            elif default_cache_swapout_strategy == CacheSwapoutStrategy.FIFO:
                while True:
                    if(mem_count+ len(chunk_data) < data_node_mem_cache * 1024):
                        break
                    victim=fifo_queue.get()
                    del mem_data_map[victim]
        # 进行存储,值为[数据，命中次数，当前时间戳]
        mem_data_map[local_path] = [chunk_data,1,time.time()]
        # 并计算本次数据的字节数
        mem_count = mem_count + len(chunk_data)
        fifo_queue.put(local_path)
        
        # 将数据块写入本地文件
        thread_store= StroeThread(local_path=local_path,chunk_data=chunk_data,semaphore=sem)
        thread_store.start()

        return "{} Store chunk {} successfully in mem~".format(hostname, local_path)

    def rm(self, dfs_path):
        local_path = data_node_dir + dfs_path
        rm_command = "rm -rf " + local_path
        os.system(rm_command)

        return "Remove chunk {} successfully~".format(local_path)

    def format(self):
        format_command = "rm -rf {}/*".format(data_node_dir)
        os.system(format_command)

        return "Format datanode successfully~"

    # 执行传过来的文件
    def run_mapreduce(self, file_name, pyscript_path, fat_path):
        fat = pd.read_csv(fat_path)
        tempfile = open("./dfs/name/midres.txt", "wt")

        reducer_res = None
        flag = False
        for idx, row in fat.iterrows():
            blk_no = row['blk_no']
            cmd = "python3 {} -mapper {}".format(pyscript_path, str(
                data_node_dir + mapreduce_node_dir + "/" + file_name + ".blk" + str(blk_no)))
            print("cmd:", cmd)
            res = str(os.popen(cmd).read())
            print("当前res:", res)
            # 说明reducer_res从未写过
            if not flag:
                flag = True
                reducer_res = res
            else:
                cmd = "python3 {} -reducer {} {}".format(pyscript_path.strip(), reducer_res.strip(), res.strip())
                cmd = str(cmd)
                print("cmd:", cmd)
                reducer_res = os.popen(cmd).read()
            tempfile.write(res)
            print("当前reducer_res:", reducer_res)
        tempfile.close()

        return "{}".format(reducer_res)


# 创建DataNode对象并启动
data_node = DataNode()
hostname = os.popen("hostname")
mem_count = 0
sem = threading.Semaphore(max_thread)  # 限制线程的最大数量为max_thread个
mem_data_map = {}
fifo_queue = Queue(maxsize=0)
data_node.run()
