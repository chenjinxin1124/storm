# ZK
/home/opt/modules/zookeeper-3.4.5-cdh5.10.0/bin/zkServer.sh start
# Hadoop
hadoop-2.7.0
## 配置
hadoop-env.sh
```
export JAVA_HOME=/home/opt/modules/jdk1.8.0_11
```
core-site.xml
```
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://bigdata-pro01:9000</value>
        <description>配置NameNode的主机地址及其端口号</description>
    </property>
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>cjx</value>
        <description>HDFS Web UI 用户名</description>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/home/opt/modules/hadoop-2.7.0/tmp</value>
        <description>其他临时目录的基础。（HDFS等相关文件的存储目录基础，很重要）</description>
    </property>
</configuration>
```
hdfs-site.xml
```
<configuration>
    <!-- 单主节点配置 -->
    <property>
        <name>dfs.replication</name>
        <value>1</value>
        <description>设置文件副本数</description>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
        <description>查看日志是否审核权限</description>
    </property>
    <property>
        <name>dfs.http.address</name>
        <value>bigdata-pro01:50070</value>
        <description>hdfs的webUI地址</description>
    </property>
</configuration>
```
slaves
```
bigdata-pro01
```
## 启动运行测试
```
/home/opt/modules/hadoop-2.7.0/bin/hdfs namenode -format
/home/opt/modules/hadoop-2.7.0/sbin/start-dfs.sh
http://bigdata-pro01:50070/
```
### 测试HDFS文件操作
1. 创建目录
```
[root@bigdata-pro01 modules]# hadoop-2.5.0/bin/hdfs dfs -mkdir -p /user/kfk/data/
在webUI查看：http://bigdata-pro01:50070/explorer.html#/
```
2. 上传文件
```
hadoop-2.5.0/bin/hdfs dfs -put /home/opt/modules/hadoop-2.5.0/etc/hadoop/core-site.xml /user/data
在webUI查看：http://bigdata-pro01:50070/explorer.html#/user/data
```
3. 查看文件
```
[root@bigdata-pro01 modules]# hadoop-2.5.0/bin/hdfs dfs -text /user/data/core-site.xml
也可以这样写
[root@bigdata-pro01 modules]# hadoop fs -text /user/data/core-site.xml
```
4. 删除目录
```
[kfk@bigdata-pro01 modules]$ hadoop-2.5.0/bin/hdfs dfs -rm -r /hbase/.tmp
```
# HBase
hbase-1.0.0-cdh5.5.0
## 配置
hbase-site.xml
```
<configuration>
	<property>
		<name>hbase.rootdir</name>
		<value>hdfs://bigdata-pro01:9000/hbase</value>
	<description>hdfs-namenode/根节点目录</description>
	</property>
	
	<property>
		<name>hbase.cluster.distributed</name>
		<value>true</value>
		<description>开启分布式</description>
	</property>
	
	<property>
		<name>hbase.zookeeper.quorum</name>
		<value>bigdata-pro01,bigdata-pro02,bigdata-pro03</value>
		<description>zookeeper所在节点</description>
	</property> 
</configuration>
```
regionservers
```
bigdata-pro01
```
## 启动运行测试
```
/home/opt/modules/hbase-1.0.0-cdh5.5.0/bin/start-hbase.sh
/home/opt/modules/hbase-1.0.0-cdh5.5.0/bin/hbase-daemon.sh start master
/home/opt/modules/hbase-1.0.0-cdh5.5.0/bin/hbase-daemon.sh start regionserver
http://bigdata-pro01:60030/rs-status
```
### 测试
```
[root@bigdata-pro01 hbase-1.0.0-cdh5.5.0]# bin/hbase shell
hbase(main):001:0> create 'WordCount','info'
hbase(main):002:0> list
TABLE                                                                                                                                                                                                              
WordCount                                                                                                                                                                                                          
1 row(s) in 0.0060 seconds

=> ["WordCount"]
```