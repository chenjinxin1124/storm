# Kafka
## 下载
http://archive.apache.org/dist/kafka/0.8.2.1/kafka_2.11-0.8.2.1.tgz
## 安装
[root@bigdata-pro02 modules]# scp -r kafka_2.11-0.8.2.1/ bigdata-pro03:/home/opt/modules/
## 配置
```
[root@bigdata-pro03 kafka_2.11-0.8.2.1]# vim config/server.properties 
broker.id=2
host.name=bigdata-pro03
num.partitions=2
zookeeper.connect=bigdata-pro01:2181,bigdata-pro02:2181,bigdata-pro03:2181
```
## 启动测试
### 依赖 zookeeper
```
/home/opt/modules/zookeeper-3.4.5-cdh5.10.0/bin/zkServer.sh start
/home/opt/modules/zookeeper-3.4.5-cdh5.10.0/bin/zkCli.sh
[zk: localhost:2181(CONNECTED) 1] ls /
[brokers, storm, zookeeper, yarn-leader-election, hadoop-ha, admin, isr_change_notification, controller_epoch, rmstore, consumers, config, hbase, transactional]
[zk: localhost:2181(CONNECTED) 2] ls /brokers
[ids, topics, seqid]
[zk: localhost:2181(CONNECTED) 3] ls /brokers/topics
[weblogs]
[zk: localhost:2181(CONNECTED) 4] ls /brokers/topics/weblogs
[partitions]
[zk: localhost:2181(CONNECTED) 5] ls /brokers/topics/weblogs/partitions
[0, 1]
[zk: localhost:2181(CONNECTED) 6] ls /brokers/topics/weblogs/partitions/0
[state]
[zk: localhost:2181(CONNECTED) 7] ls /brokers/topics/weblogs/partitions/0/state
[]
[zk: localhost:2181(CONNECTED) 4] rmr /brokers/topics/weblogs
```
### 启动 Kafka
```
[root@bigdata-pro01 kafka_2.11-0.8.2.1]# bin/kafka-server-start.sh config/server.properties
```
创建 topic(副本数1，分片数1，名字：test)
```
bin/kafka-topics.sh --create --zookeeper bigdata-pro01:2181,bigdata-pro02:2181,bigdata-pro03:2181 --replication 1 --partitions 1 --topic test
bin/kafka-topics.sh --create --zookeeper bigdata-pro01:2181,bigdata-pro02:2181,bigdata-pro03:2181 --replication 1 --partitions 1 --topic test2
```
# Storm集成Kafka
http://storm.apache.org/releases/1.2.3/storm-kafka.html
```
<dependency>
    <groupId>org.apache.storm</groupId>
    <artifactId>storm-kafka</artifactId>
    <version>1.2.3</version>
</dependency>
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka_2.11</artifactId>
    <version>0.8.2.1</version>
    <exclusions>
        <exclusion>
            <artifactId>zookeeper</artifactId>
            <groupId>org.apache.zookeeper</groupId>
        </exclusion>

        <exclusion>
            <artifactId>log4j</artifactId>
            <groupId>log4j</groupId>
        </exclusion>

        <exclusion>
            <artifactId>slf4j-log4j12</artifactId>
            <groupId>org.slf4j</groupId>
        </exclusion>

        <exclusion>
            <artifactId>scala-library</artifactId>
            <groupId>org.scala-lang</groupId>
        </exclusion>
        <exclusion>
            <artifactId>jopt-simple</artifactId>
            <groupId>net.sf.jopt-simple</groupId>
        </exclusion>
        <exclusion>
            <artifactId>zkclient</artifactId>
            <groupId>com.101tec</groupId>
        </exclusion>
    </exclusions>
</dependency>
```
## 解决包冲突
SLF4J: Detected both log4j-over-slf4j.jar AND slf4j-log4j12.jar on the class path, preempting StackOverflowError.
```
Project Structure -> Project Settings -> Libraries -> 删除 Maven:org.slf4j-over-slf4j
```