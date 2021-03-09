# storm
# 系统
```
# cat /etc/redhat-release
CentOS Linux release 7.9.2009 (Core)
```
## storm安装步骤
### 安装Storm依赖的软件
#### Java7+
```
# vim /etc/profile
# java -version
java version "1.8.0_11"
Java(TM) SE Runtime Environment (build 1.8.0_11-b12)
Java HotSpot(TM) 64-Bit Server VM (build 25.11-b03, mixed mode)
```
#### Python(系统自带)
```
# python --version
Python 2.7.5
```
### 搭建ZooKeeper集群
zookeeper-3.4.5-cdh5.10.0
#### 启动三台zk服务
/home/opt/modules/zookeeper-3.4.5-cdh5.10.0/bin/zkServer.sh start
#### zk操作
```
/home/opt/modules/zookeeper-3.4.5-cdh5.10.0/bin/zkCli.sh
[zk: localhost:2181(CONNECTED) 0] ls /
[controller_epoch, brokers, zookeeper, yarn-leader-election, hadoop-ha, rmstore, admin, isr_change_notification, consumers, config, hbase]
```
### 安装及配置Storm
#### 下载
http://archive.apache.org/dist/storm/apache-storm-1.2.3/apache-storm-1.2.3.tar.gz
#### 官网文档安装指南
http://storm.apache.org/releases/1.2.3/Setting-up-a-Storm-cluster.html
#### 安装
```
# tar -zxf apache-storm-1.2.3.tar.gz -C /home/opt/modules/
```
#### 配置
```
默认配置：https://github.com/apache/storm/blob/v1.2.3/conf/defaults.yaml

apache-storm-1.2.3]# vim conf/storm.yaml
 storm.zookeeper.servers:
     - "bigdata-pro01"
     - "bigdata-pro02"
     - "bigdata-pro03"
 
 nimbus.seeds: ["bigdata-pro01", "bigdata-pro02"]

 supervisor.slots.ports:
    - 6700
    - 6701
    - 6702
    - 6703

 ui.port: 8888
```
#### 分发
```
modules]# scp -r apache-storm-1.2.3/ bigdata-pro02:/home/opt/modules/
modules]# scp -r apache-storm-1.2.3/ bigdata-pro03:/home/opt/modules/
```
#### 启动服务
```
[root@bigdata-pro01 modules]# /home/opt/modules/apache-storm-1.2.3/bin/storm nimbus
[root@bigdata-pro01 modules]# /home/opt/modules/apache-storm-1.2.3/bin/storm supervisor
[root@bigdata-pro01 modules]# /home/opt/modules/apache-storm-1.2.3/bin/storm ui

[root@bigdata-pro02 modules]# /home/opt/modules/apache-storm-1.2.3/bin/storm nimbus
[root@bigdata-pro02 modules]# /home/opt/modules/apache-storm-1.2.3/bin/storm supervisor

[root@bigdata-pro03 modules]# /home/opt/modules/apache-storm-1.2.3/bin/storm supervisor
```
#### UI界面
http://bigdata-pro01:8888/index.html
