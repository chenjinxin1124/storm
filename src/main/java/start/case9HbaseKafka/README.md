# 事务，数据持久化到HBASE
# ZK
/home/opt/modules/zookeeper-3.4.5-cdh5.10.0/bin/zkServer.sh start
# Hadoop
/home/opt/modules/hadoop-2.7.0/sbin/start-dfs.sh
# HBase
/home/opt/modules/hbase-1.0.0-cdh5.5.0/bin/start-hbase.sh
# Kafka
/home/opt/modules/kafka_2.11-0.8.2.1/bin/kafka-server-start.sh /home/opt/modules/kafka_2.11-0.8.2.1/config/server.properties
## 测试
```
[root@bigdata-pro01 hbase-1.0.0-cdh5.5.0]# bin/hbase shell
hbase(main):001:0> truncate 'storm'
```
### 生产数据
KafkaProducer
### 计算
TridentKafkaWordCount
# 问题
log4j相关。删除*log4j-over*包