# 安装使用
https://www.jianshu.com/p/ff915e062f86
## 安装
```
# yum -y install python-setuptools
# easy_install supervisor
# supervisord -v
4.2.2
默认安装的版本是最新版
```
## 配置 & 启动服务
```
# mkdir -p /etc/supervisor/confs
生成配置文件(supervisord.conf)
# echo_supervisord_conf > /etc/supervisor/supervisord.conf
```
### 修改配置文件
分号后边的表示注释
```
# vim /etc/supervisor/supervisord.conf
[include]
files = /etc/supervisor/confs/*.conf

启动服务
# supervisord -c /etc/supervisor/supervisord.conf
# ps -ef | grep supervisord
```
### 创建脚本
```
storm.conf
```
### 发送运行
```
scp supervisor/storm.conf root@bigdata-pro01:/etc/supervisor/confs/
scp supervisor/storm.conf root@bigdata-pro02:/etc/supervisor/confs/
scp supervisor/storm.conf root@bigdata-pro03:/etc/supervisor/confs/

更新新的配置到supervisord    
supervisorctl update
服务状态
supervisorctl status

[root@bigdata-pro01 ~]# supervisorctl status
storm-logviewer                  STOPPED   Not started
storm-nimbus                     STOPPED   Not started
storm-supervisor                 STOPPED   Not started
storm-ui                         STOPPED   Not started
[root@bigdata-pro01 ~]# supervisorctl
storm-logviewer                  STOPPED   Not started
storm-nimbus                     STOPPED   Not started
storm-supervisor                 STOPPED   Not started
storm-ui                         STOPPED   Not started
supervisor> start storm-nimbus
supervisor> start storm-ui
supervisor> start storm-supervisor
supervisor> start storm-logviewer
supervisor> status
storm-logviewer                  RUNNING   pid 11222, uptime 0:01:12
storm-nimbus                     RUNNING   pid 10607, uptime 0:03:27
storm-supervisor                 RUNNING   pid 11004, uptime 0:02:09
storm-ui                         RUNNING   pid 10813, uptime 0:02:50

[root@bigdata-pro02 ~]# supervisorctl start storm-nimbus
[root@bigdata-pro02 ~]# supervisorctl start storm-supervisor
[root@bigdata-pro02 ~]# supervisorctl start storm-logviewer
[root@bigdata-pro02 ~]# supervisorctl status

[root@bigdata-pro03 ~]# supervisorctl start storm-supervisor
[root@bigdata-pro03 ~]# supervisorctl start storm-logviewer

supervisorctl stop all
```
### 常用命令
```
服务状态
supervisorctl status

更新新的配置到supervisord    
supervisorctl update

重新启动配置中的所有程序
supervisorctl reload

启动某个进程
supervisorctl start [program_name]

进入命令行模式
supervisorctl

停止某一进程
pervisorctl stop [program_name]

重启某一进程
supervisorctl restart [program_name]

停止全部进程
supervisorctl stop all
```