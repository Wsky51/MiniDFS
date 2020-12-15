from enum import Enum

dfs_replication = 2
dfs_blk_size = 4096  # * 单位字节,此可以理论上可以设为任意大，但务必比PACK_SIZE（最大1500）设置要大

data_node_mem_cache=1024 #单位MB，1024MB=1GB,设置为0则代表data node不用缓存加速
name_node_mem_cache=512 #单位MB,如512MB，设置为0则代表name node不用缓存加速
max_thread=4

# NameNode和DataNode数据存放位置
name_node_dir = "./dfs/name"
data_node_dir = "./dfs/data"
mapreduce_node_dir="/mapreduce"

data_node_port = 14269  # DataNode程序监听端口
name_node_port = 24269  # NameNode监听端口

# 集群中的主机列表
host_list = ['thumm04', 'thumm02',
             'thumm03']  # ['thumm01', 'thumm02', 'thumm03', 'thumm04', 'thumm05', 'thumm06', 'thumm07', 'thumm08']
name_node_host = "localhost"

BUF_SIZE = dfs_blk_size * 2

#设置每一次sokct发送的数据量，推荐设置为1024，警告:该值请勿设置大于1500以上
PACK_SIZE = 1024
PACK_SIZE_FMT = str(PACK_SIZE) + 's'


#缓存换出策略
class CacheSwapoutStrategy(Enum):
    #采用最近最久未使用策略
    LRU=1
    #采用最近最少使用策略
    LFU=2
    #采用先进先出策略
    FIFO=3

#默认缓存换出策略为LRU
default_cache_swapout_strategy=CacheSwapoutStrategy.LRU