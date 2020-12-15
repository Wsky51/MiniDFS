import math
import os
import socket
import time
from io import StringIO
import threading

import pandas as pd
from common import *
from utils import *

class Client:
    def __init__(self):
        self.name_node_sock = socket.socket()
        self.name_node_sock.connect((name_node_host, name_node_port))
        self.name_node_sock.setblocking(True)
        self.sem=threading.Semaphore(4)

    def __del__(self):
        self.name_node_sock.close()

    def ls(self, dfs_path):
        # 向NameNode发送请求，查看dfs_path下文件或者文件夹信息
        try:
            cmd = "ls {}".format(dfs_path)
            # self.name_node_sock.send(bytes(cmd, encoding='utf-8'))
            strong_sck_send(self.name_node_sock, str_encode_utf8(cmd))
            response_msg = strong_sck_recv(self.name_node_sock)
            print(utf8_decode_str(response_msg))
        except Exception as e:
            print(e)
        finally:
            pass

    def copyFromLocal(self, local_path, dfs_path):
        file_size = os.path.getsize(local_path)
        print("File size: {}".format(file_size))

        request = "new_fat_item {} {}".format(dfs_path, file_size)
        print("Request: {}".format(request))

        # 从NameNode获取一张FAT表
        # self.name_node_sock.send(bytes(request, encoding='utf-8'))
        strong_sck_send(self.name_node_sock, str_encode_utf8(request))
        # fat_pd = self.name_node_sock.recv(BUF_SIZE)
        fat_pd = strong_sck_recv(self.name_node_sock)

        # 打印FAT表，并使用pandas读取
        # fat_pd = str(fat_pd, encoding='utf-8')
        fat_pd = utf8_decode_str(fat_pd)
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))  # 从namenode那拿到fat表

        # 根据FAT表逐个向目标DataNode发送数据块
        old_blk = -1
        data = None
        fp = open(local_path)

        for idx, row in fat.iterrows():
            host = row['host_name']
            if row['blk_no'] != old_blk:
                data = fp.read(int(row['blk_size']))
                old_blk = row['blk_no']
            data_node_sock = socket.socket()

            print("host:", host, ",data_node_port:", data_node_port)
            data_node_sock.connect((host, data_node_port))  # 和不同主机建立连接
            data_node_sock.setblocking(True)
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])

            request = "store {}".format(blk_path)  # 类似于指令"store /test/testdfs.txt.blk2"
            print("client将向：", host, "发送数据，指令为：", request)
            # data_node_sock.send(bytes(request, encoding='utf-8'))
            strong_sck_send(data_node_sock, str_encode_utf8(request))
            # time.sleep(0.1)  # 两次传输需要间隔一段时间，避免粘包
            # data_node_sock.send(bytes(data, encoding='utf-8'))
            strong_sck_send(data_node_sock, str_encode_utf8(data))

            response_msg = strong_sck_recv(data_node_sock)
            print(utf8_decode_str(response_msg))
            data_node_sock.close()
        time.sleep(0.5)
        fp.close()
        return fat

    def copyToLocal(self, dfs_path, local_path):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))

        # 从NameNode获取一张FAT表
        # self.name_node_sock.send(bytes(request, encoding='utf-8'))
        strong_sck_send(self.name_node_sock, str_encode_utf8(request))

        # fat_pd = self.name_node_sock.recv(BUF_SIZE)
        fat_pd = strong_sck_recv(self.name_node_sock)

        # 打印FAT表，并使用pandas读取
        fat_pd = utf8_decode_str(fat_pd)
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        # 根据FAT表逐个从目标DataNode请求数据块，写入到本地文件中
        fp = open(local_path, "w")
        old_blk = -1

        count=0
        for idx, row in fat.iterrows():
            count+=1
            if row['blk_no'] == old_blk:  # 和旧的块号相同说明是冗余块，那么可以略过
                continue
            old_blk = row['blk_no']
            print("将弄出这个块：", row)

            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))

            data_node_sock.setblocking(True)

            blk_path = dfs_path + ".blk{}".format(row['blk_no'])  # 命名类似于："/test/testdfs.txt"+".blk1"

            request = "load {}".format(blk_path)

            # data_node_sock.send(bytes(request, encoding='utf-8'))
            strong_sck_send(data_node_sock, str_encode_utf8(request))
            print("给data node发送指令完毕")
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
            data = strong_sck_recv(data_node_sock) #一直进行接收数据

            data = utf8_decode_str(data)

            if count>10:
                count=0
                time.sleep(0.2)
            fp.write(data)
            fp.flush()
            data_node_sock.close()
        fp.close()

    def rm(self, dfs_path):
        request = "rm_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))

        # 从NameNode获取改文件的FAT表，获取后删除
        # self.name_node_sock.send(bytes(request, encoding='utf-8'))
        strong_sck_send(self.name_node_sock, str_encode_utf8(request))

        # fat_pd = self.name_node_sock.recv(BUF_SIZE)
        fat_pd = strong_sck_recv(self.name_node_sock)

        # 打印FAT表，并使用pandas读取
        # fat_pd = str(fat_pd, encoding='utf-8')
        fat_pd = utf8_decode_str(fat_pd)
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        # 根据FAT表逐个告诉目标DataNode删除对应数据块
        for idx, row in fat.iterrows():
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])

            request = "rm {}".format(blk_path)
            # data_node_sock.send(bytes(request, encoding='utf-8'))
            strong_sck_send(data_node_sock, str_encode_utf8(request))

            # response_msg = data_node_sock.recv(BUF_SIZE)
            response_msg = strong_sck_recv(data_node_sock)
            print(utf8_decode_str(response_msg))

            data_node_sock.close()

    def format(self):
        request = "format"
        print(request)

        # self.name_node_sock.send(bytes(request, encoding='utf-8'))
        strong_sck_send(self.name_node_sock, str_encode_utf8(request))
        # print(str(self.name_node_sock.recv(BUF_SIZE), encoding='utf-8'))
        print(utf8_decode_str(strong_sck_recv(self.name_node_sock)))

        for host in host_list:
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))

            # data_node_sock.send(bytes("format", encoding='utf-8'))
            strong_sck_send(data_node_sock, str_encode_utf8("format"))
            # print(utf8_decode_str(data_node_sock.recv(BUF_SIZE)))
            print(utf8_decode_str(strong_sck_recv(data_node_sock)))

            data_node_sock.close()

    def test_socket(self, data):
        cmd = "testSocket {}".format(data)
        # self.name_node_sock.send(bytes(request, encoding='utf-8'))
        strong_sck_send(self.name_node_sock, str_encode_utf8(cmd))
        recv_data = strong_sck_recv(self.name_node_sock)
        print("收到的数据为：", recv_data)
        print("数据大小为:", len(recv_data))

    def map_reduce(self, pyscript_path, input_path, output_path):
        file_name = os.path.split(input_path)[1]
        dfs_path = mapreduce_node_dir + "/" + file_name

        # 进行文件的分布式分配任务
        fat = self.copyFromLocal(input_path, dfs_path)

        true_block = int(fat.shape[0] / dfs_replication)
        host_size = len(host_list)
        # 计算每个datanode应该处理多少数据
        single_block = math.ceil(true_block / host_size)
        print("true block:", true_block, "singl_block:", single_block)

        host_blk_fat = {}
        blk_mark = [False] * true_block
        for idx, row in fat.iterrows():
            # 如果一个块已经被分配了那就没必要继续了
            if blk_mark[idx // dfs_replication]:
                continue
            blk_no = row["blk_no"]
            host_name = row["host_name"]
            blk_size = row["blk_size"]
            if not host_name in host_blk_fat:
                temp_fat = pd.DataFrame([blk_no, host_name, blk_size]).T
                temp_fat.columns = fat.columns
                host_blk_fat[host_name] = temp_fat
                blk_mark[idx // dfs_replication] = True
            elif host_blk_fat[host_name].shape[0] < single_block:
                index = host_blk_fat[host_name].shape[0]
                host_blk_fat[host_name].loc[index] = [blk_no, host_name, blk_size]
                blk_mark[idx // dfs_replication] = True
        print("host_blk_fat:\n", host_blk_fat)

        fin_res = None
        flag = False
        for host in host_blk_fat:
            local_path = "./temp/" + host + "_mr_fat.txt"
            # 若目录不存在则创建新目录
            os.system("mkdir -p {}".format(os.path.dirname(local_path)))
            print("currenct csv:\n", host_blk_fat[host].to_csv(index=False))
            host_blk_fat[host].to_csv(local_path, index=False)

            # 传送脚本和该节点的fat文件到远端
            # 自行约定的把文件放到/MyDFS/dfs/name/
            print("scpcmd1:", "scp " + pyscript_path + " " + host + ":~/MyDFS/dfs/name/" + pyscript_path)
            print("scpcmd2:", "scp " + local_path + " " + host + ":~/MyDFS/dfs/name/" + host + "_mr_fat.txt")
            os.system("scp " + pyscript_path + " " + host + ":~/MyDFS/dfs/name/temp_" + pyscript_path)
            os.system("scp " + local_path + " " + host + ":~/MyDFS/dfs/name/" + host + "_mr_fat_temp.txt")

            # 分别给datanode节点发送消息让其开始跑任务
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))  # 和不同主机建立连接

            # 告知data_node让其执行给他上传的脚本
            request = "run_mapreduce {} {} {}".format(file_name, "./dfs/name/temp_" + pyscript_path,
                                                      "./dfs/name/" + host + "_mr_fat_temp.txt")
            strong_sck_send(data_node_sock, str_encode_utf8(request))

            response_msg = strong_sck_recv(data_node_sock)
            temp_res = utf8_decode_str(response_msg)
            print("成功接收到temp_res当前信息：", temp_res)

            if not flag:
                flag = True
                fin_res = temp_res
            else:
                cmd = "python3 {} -reducer {} {}".format(pyscript_path.strip(), fin_res.strip(), temp_res.strip())
                cmd = str(cmd)
                print("client cmd:", cmd)
                fin_res = os.popen(cmd).read()
            data_node_sock.close()
        output_file = open(output_path, "wt")
        output_file.write(fin_res)
        output_file.close()
        print("MapReduce 结束，最终结果为：", fin_res)


# 解析命令行参数并执行对于的命令
import sys
argv = sys.argv
argc = len(argv) - 1

client = Client()

cmd = argv[1]

if cmd == '-ls':
    if argc == 2:
        dfs_path = argv[2]
        client.ls(dfs_path)
    else:
        print("Usage: python client.py -ls <dfs_path>")
elif cmd == "-rm":
    if argc == 2:
        dfs_path = argv[2]
        client.rm(dfs_path)
    else:
        print("Usage: python client.py -rm <dfs_path>")
elif cmd == "-copyFromLocal":
    if argc == 3:
        local_path = argv[2]
        dfs_path = argv[3]
        client.copyFromLocal(local_path, dfs_path)
    else:
        print("Usage: python client.py -copyFromLocal <local_path> <dfs_path>")
elif cmd == "-copyToLocal":
    if argc == 3:
        dfs_path = argv[2]
        local_path = argv[3]
        client.copyToLocal(dfs_path, local_path)
    else:
        print("Usage: python client.py -copyFromLocal <dfs_path> <local_path>")
elif cmd == "-format":
    client.format()

elif cmd == "-testSocket":
    if argc == 2:
        data = argv[2]
        client.test_socket(data)
    else:
        print("Usage: python client.py -testSocket <data>")

elif cmd == "-mapReduce":
    if argc == 4:
        pyscipt_path = argv[2]
        input_path = argv[3]
        output_path = argv[4]
        client.map_reduce(pyscipt_path, input_path, output_path)
    else:
        print("Usage: python client.py -mapReduce <pyscipt_path> <input_path> <output_path>")

else:
    print("Undefined command: {}".format(cmd))
    print("Usage: python client.py <-ls | -copyFromLocal | -copyToLocal | -rm | -format> other_arguments")
