# 启动服务
zookeeper
```
[root@bigdata-pro01 ~]# /home/opt/modules/zookeeper-3.4.5-cdh5.10.0/bin/zkServer.sh start
```
hadoop
```
[root@bigdata-pro01 ~]# /home/opt/modules/hadoop-2.7.0/sbin/start-dfs.sh
```
HBase
```
[root@bigdata-pro01 ~]# /home/opt/modules/hbase-1.0.0-cdh5.5.0/bin/start-hbase.sh
```
Storm
```
[root@bigdata-pro01 ~]# supervisord -c /etc/supervisor/supervisord.conf
[root@bigdata-pro01 ~]# supervisorctl status

[root@bigdata-pro01 ~]#
supervisorctl start storm1.2.1-ui
supervisorctl start storm1.2.1-nimbus
supervisorctl start storm1.2.1-supervisor
supervisorctl start storm1.2.1-logviewer
supervisorctl start storm1.2.1-drpc
supervisorctl status
```
MySQL
```
[root@bigdata-pro01 ~]# systemctl start docker
docker run \
--restart=always \
--name mysql57 \
--hostname mysql57 \
-p 3306:3306 \
-e 'MYSQL_ROOT_PASSWORD=123456' \
-e 'MYSQL_ROOT_HOST=%' \
-e 'MYSQL_DATABASE=db_test' \
-d mysql:5.7 \
--character-set-server=utf8mb4 \
--collation-server=utf8mb4_unicode_ci
```
Kafka
```
[root@bigdata-pro01 ~]# /home/opt/modules/kafka_2.11-0.8.2.1/bin/kafka-server-start.sh /home/opt/modules/kafka_2.11-0.8.2.1/config/server.properties
```
## 创建HBASE表
[root@bigdata-pro01 ~]# /home/opt/modules/hbase-1.0.0-cdh5.5.0/bin/hbase shell
```
hbase(main):001:0> create 'trun_shop_ranking','info'
hbase(main):002:0> create 'trun_all_amts','info'
hbase(main):003:0> create 'xTime_all_amts','info'
```
## MySQL表
```
[root@bigdata-pro01 ~]# docker exec -it mysql57utf8 /bin/bash
root@mysql:/# mysql -uroot -p123456
use storm;
create table shop_ranking(
shop_id varchar(30) not null,
shop_name varchar(30) not null,
shop_amtSum varchar(30) not null,
shop_orderSum varchar(30) not null
);
```
## 创建Topic
```
[root@bigdata-pro01 kafka_2.11-0.8.2.1]# bin/kafka-topics.sh --create --zookeeper bigdata-pro01:2181 --replication-factor 1 --partitions 1 --topic hbaseTest
[root@bigdata-pro01 kafka_2.11-0.8.2.1]# bin/kafka-topics.sh --create --zookeeper bigdata-pro01:2181 --replication-factor 1 --partitions 1 --topic hbaseTest2
```