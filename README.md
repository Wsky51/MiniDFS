# 使用方法

### 一、系统架构

#### 1.1. 介绍

该DFS实现了简单分布式文件系统的功能，包含了NameNode、DataNode、CLient三部分，其中NameNode负责记录文件块的位置（FAT表）， DataNode负责数据的存储与读取，而Client则是用户与DFS交互的接口。

该DFS主要实现了ls / copyFromLocal /copyToLocal/mapReduce功能, 该DFS有如下优化或功能功能

- 实现复数个块副本。在common.py文件中设置dfs_replication的大小（目前设置为2）

- 实现统一的socket收发接口，解决粘包问题

- 实现缓存策略，利用内存缓存命中加速文件数据的读取，并可由用户配置缓存大小（设为0则不利用缓存加速，可在common.py文件中设置mem_cache的大小），提供LRU, LFU算法供用户选择执行不同的缓存块换出策略（目前只实现了这两种换出策略）

- 通过多线程,semaphore实现文件的异步存取，避免主线程由于写磁盘造成的时间开销

- 配合wuyi-master-node-web和wuyi-master-node实现DFS系统的动态监控，实时展示DFS内存占用率，空余内存大小，DFS磁盘使用情况，当前节点的状态信息（如是否已启动,pid号等），集群的信息总览（各个节点的端口号，集群节点，集群文件信息，块大小）等等



#### 1.2. 目录结构
- MiniDFS : 根目录
    - dfs : DFS文件夹，用于存放DFS数据
        - name : 存放NameNode数据
        - data : 存放DataNode数据
    -test : 存放测试样例
        -test.txt
    - common.py  : 全局变量
    - name_node.py : NameNode程序
    - data_node.py : DataNode程序
    - client.py : Client程序，用于用户与DFS交互
    - utils.py : 工具类包，公共函数放在此处

#### 1.3. 模块功能

- name_node.py
    - 保存文件的块存放位置信息
    - 获取文件/目录信息
    - get_fat_item： 获取文件的FAT表项
    - new_fat_item： 根据文件大小创建FAT表项
    - rm_fat_item： 删除一个FAT表项
    - format: 删除所有FAT表项
    - get_all_data : 实现了所有服务节点数据信息的采集并封装成json数据发送给wuyi-master-node

- data_node.py
    - load ：加载数据块
    - store ：保存数据块
    - rm ：删除数据块
    - format ：删除所有数据块
    - run_mapreduce ：执行MR任务

- client.py
    - ls : 查看当前目录文件/目录信息
    - copyFromLocal : 从本地复制数据到DFS
    - copyToLocal ： 从DFS复制数据到本地
    - rm ： 删除DFS上的文件
    - format ：格式化DFS
    - map_reduce ：执行MR任务
### 二、操作步骤

0. 进入MiniDFS目录
```
$ cd MiniDFS
``
1. 启动NameNode

```sh
$ python3 name_node.py
```

2. 启动DataNode

```
$ python3 data_node.py
```

3. 测试指令

- ls <dfs_path> : 显示当前目录/文件信息
- copyFromLocal <local_path> <dfs_path> : 从本地复制文件到DFS
- copyToLocal <dfs_path> <local_path> : 从DFS复制文件到本地
- rm <dfs_path> : 删除DFS上的文件
- format : 格式化DFS
- map_reduce：执行MR

首先从本地复制一个文件到DFS

```
$ python3 client.py -copyFromLocal test/test.txt /test/test.txt
File size: 8411
Request: new_fat_item /test/test.txt 8411
Fat:
blk_no,host_name,blk_size
0,localhost,4096
1,localhost,4096
2,localhost,219
```

启动blk_no为块号， host_name为该数据块存放的主机名字，blk_size为块的大小

查看DFS的根目录内容

```sh
$ python3 client.py -ls /test
test.txt
```

如果ls后面跟的是文件，可以看到该文件FAT信息

```sh
$ python3 client.py -ls /test/test.txt
blk_no,host_name,blk_size
0,localhost,4096
1,localhost,4096
2,localhost,219
```

接着再从DFS复制到本地，存储为test2.txt

```sh
$ python3 client.py -copyToLocal /test/test.txt test/test2.txt
Request: get_fat_item /test/test.txt
Fat:
blk_no,host_name,blk_size
0,localhost,4096
1,localhost,4096
2,localhost,219
```

校验前后文件是否一致

```sh
$ diff test/test.txt test/test2.txt
$
```

可以看到前后文件一致

接着测试删除文件

```sh
$ python3 client.py -rm /test/test.txt
Request: rm_fat_item /test/test.txt
Fat:
blk_no,host_name,blk_size
0,localhost,4096
1,localhost,4096
2,localhost,219

b'Remove chunk ./dfs/data/test/test.txt.blk0 successfully~'
b'Remove chunk ./dfs/data/test/test.txt.blk1 successfully~'
b'Remove chunk ./dfs/data/test/test.txt.blk2 successfully~'
```

再次查看/test文件夹
```
$ python3 client.py -ls /test

```
原先的test.txt已删除

接着测试format
```sh
$ python3 client.py -format
format
Format namenode successfully~
Format datanode successfully~
```

查看根目录
```sh
$ python3 client.py -ls /

```
所有文件/文件夹均被删除
