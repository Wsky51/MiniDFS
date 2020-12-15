import datetime
import json
import math
import os
import socket
import re
import functools
import numpy as np
import pandas as pd
from common import *
from utils import *


# NameNode功能
# 1. 保存文件的块存放位置信息
# 2. ls ： 获取文件/目录信息
# 3. get_fat_item： 获取文件的FAT表项
# 4. new_fat_item： 根据文件大小创建FAT表项
# 5. rm_fat_item： 删除一个FAT表项
# 6. format: 删除所有FAT表项

class NameNode:
    def run(self):  # 启动NameNode
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", name_node_port))
            listen_fd.listen(5)
            listen_fd.setblocking(True)
            print("Name node started")
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("connected by {}".format(addr))

                try:
                    # 获取请求方发送的指令
                    # request = str(sock_fd.recv(128), encoding='utf-8')
                    request = str(strong_sck_recv(sock_fd), encoding='utf-8')
                    print("request:", request)
                    request = request.split()  # 指令之间使用空白符分割
                    print("Request: {}".format(request))

                    cmd = request[0]  # 指令第一个为指令类型

                    if cmd == "ls":  # 若指令类型为ls, 则返回DFS上对于文件、文件夹的内容
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.ls(dfs_path)
                    elif cmd == "get_fat_item":  # 指令类型为获取FAT表项
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.get_fat_item(dfs_path)
                    elif cmd == "new_fat_item":  # 指令类型为新建FAT表项
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        file_size = int(request[2])
                        response = self.new_fat_item(dfs_path, file_size)
                    elif cmd == "rm_fat_item":  # 指令类型为删除FAT表项
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.rm_fat_item(dfs_path)
                    elif cmd == "format":
                        response = self.format()
                    elif cmd == "testSocket":
                        data = request[1]
                        response = self.test_socket(data)
                    elif cmd == "getAllData":
                        response = self.get_all_data()
                    else:  # 其他位置指令
                        response = "Undefined command: " + " ".join(request)

                    print("Response: {}".format(response))
                    # sock_fd.send(bytes(response, encoding='utf-8'))
                    strong_sck_send(sock_fd, str_encode_utf8(response))
                except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
                    break
                except Exception as e:  # 如果出错则打印错误信息
                    print(e)
                finally:
                    sock_fd.close()  # 释放连接
        except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
            pass
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            listen_fd.close()  # 释放连接

    def ls(self, dfs_path):
        local_path = name_node_dir + dfs_path
        # 如果文件不存在，返回错误信息
        if not os.path.exists(local_path):
            return "No such file or directory: {}".format(dfs_path)

        if os.path.isdir(local_path):
            # 如果目标地址是一个文件夹，则显示该文件夹下内容
            dirs = os.listdir(local_path)
            response = " ".join(dirs)
        else:
            # 如果目标是文件则显示文件的FAT表信息
            with open(local_path) as f:
                response = f.read()

        return response

    def get_fat_item(self, dfs_path):
        # 获取FAT表内容
        local_path = name_node_dir + dfs_path
        print("localpath:", local_path)
        response = pd.read_csv(local_path)
        return response.to_csv(index=False)

    def new_fat_item(self, dfs_path, file_size):
        nb_blks = int(math.ceil(file_size / dfs_blk_size))
        print(file_size, nb_blks)

        print("hello")

        # todo 如果dfs_replication为复数时可以新增host_name的数目
        data_pd = pd.DataFrame(columns=['blk_no', 'host_name', 'blk_size'])

        for i in range(nb_blks):
            blk_no = i
            host_name = np.random.choice(host_list, size=dfs_replication, replace=False)
            blk_size = min(dfs_blk_size, file_size - i * dfs_blk_size)

            for j in range(len(host_name)):
                data_pd.loc[2 * i + j] = [blk_no, host_name[j], blk_size]

        # 获取本地路径
        local_path = name_node_dir + dfs_path  # 也就是"./dfs/name"+"/test/testdfs.txt"

        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 保存FAT表为CSV文件
        data_pd.to_csv(local_path, index=False)
        # 同时返回CSV内容到请求节点
        return data_pd.to_csv(index=False)

    def rm_fat_item(self, dfs_path):
        local_path = name_node_dir + dfs_path
        response = pd.read_csv(local_path)
        os.remove(local_path)
        return response.to_csv(index=False)

    def format(self):
        format_command = "rm -rf {}/*".format(name_node_dir)
        os.system(format_command)
        return "Format namenode successfully~"

    def test_socket(self, data):
        print("我接受到的发送端的数据为：", data)
        return 2500 * "a"

    def get_all_data(self):
        res = {}

        # 获取系统时间
        curr_time = datetime.datetime.now()
        time_str = datetime.datetime.strftime(curr_time, '%Y-%m-%d %H:%M:%S')
        res["data_node_info"]=[]

        # 获取各个节点的信息
        for host in host_list:
            hostinfo={}

            #获取pid号
            pid = getPid(host)
            hostinfo["host"]=host
            hostinfo["pid"]=pid


            '''获取各个节点当前的内存信息 grep MemFree /proc/meminfo | awk '{print $2$3}'
            获取cat /proc/meminfo | grep MemTotal |awk '{print $2$3}'
            '''
            cmd = "ssh " + host + " \"grep MemFree /proc/meminfo\""
            print("cmd:", cmd)
            freeMem = os.popen(cmd).read()
            freeMem=re.findall("\d+", freeMem)[0]
            hostinfo["freeMem"]=str(int(freeMem)//1024)+"MB"

            cmd = "ssh " + host + " \"cat /proc/meminfo | grep MemTotal\""
            print("cmd:", cmd)
            totalMem = os.popen(cmd).read()
            totalMem=re.findall("\d+", totalMem)[0]
            hostinfo["totalMem"]=str(int(totalMem)//1024)+"MB"

            #计算内存占比
            mem_prop = str(format(100 * (1-int(freeMem)/int(totalMem)) , ".1f")) + "%"
            hostinfo["mem_prop"]=mem_prop

            #获取cpu核的个数 cat /proc/cpuinfo | grep "cpu cores" | uniq | awk '{print $4}'
            cmd = "ssh " + host + " 'cat /proc/cpuinfo | grep \"cpu cores\" | uniq'"
            cpu_core = os.popen(cmd).read()
            cpu_core = re.findall("\d+", cpu_core)[0]
            hostinfo["cpu_core"]=cpu_core

            #计算dfs used
            cmd = "ssh " +host+ " 'du -sh ./MyDFS/dfs/data'"
            dfs_used=os.popen(cmd).read()
            hostinfo["dfs_used"]=dfs_used.split()[0].strip()


            res["data_node_info"].append(hostinfo)

        
        #查看有哪些文件
        local_path = name_node_dir + "/"
        files=walk_files(local_path)
        res["files:"]=files

        res["curr_time"] = time_str
        res["host_list"] = host_list
        res["data_node_port"] = data_node_port
        res["name_node_port"] = name_node_port
        res["dfs_replication"] = dfs_replication
        res["dfs_blk_size"] = dfs_blk_size
        list.sort(res["data_node_info"], key=functools.cmp_to_key(comparedata),reverse=True)
        return json.dumps(res)

# 创建NameNode并启动
name_node = NameNode()
name_node.run()

