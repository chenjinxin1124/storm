# Storm客户端命令
http://storm.apache.org/releases/1.2.3/Command-line-client.html
## 可视化日志(在所有启动了 supervisor 的机器启动 logviewer)
```
http://bigdata-pro01:8000/daemonlog?file=supervisor.log
启动的那台机器才可以查看到日志，不启动就查看不了对应机器的日志
[root@bigdata-pro01 modules]# /home/opt/modules/apache-storm-1.2.3/bin/storm logviewer
[root@bigdata-pro02 modules]# /home/opt/modules/apache-storm-1.2.3/bin/storm logviewer
[root@bigdata-pro03 modules]# /home/opt/modules/apache-storm-1.2.3/bin/storm logviewer
```