# DRPC服务配置与客户端访问
## 配置
```
# vim /home/opt/modules/apache-storm-1.2.3/conf/storm.yaml

## Locations of the drpc servers
 drpc.servers:
     - "bigdata-pro01"
     - "bigdata-pro02"
```
## 启动drpc服务(在supervisor添加drpc服务)
scp supervisor/storm.conf root@bigdata-pro01:/etc/supervisor/confs/
```
更新新的配置到supervisord: supervisorctl update
# supervisorctl start storm-drpc
```
## 打包发布
start/case6TridentTest/TridentOperTest.java
### 打包(先删除：META-INF目录)
```
Project Structure 
-> Project Setting -> Artifacts -> '+' -> JAR -> From modules with dependencies... 
-> Module: Storm, Main Class: TridentOperTest -> OK 
-> storm:jar -> Output Layout -> 删除'storm'comlile output以外的包
-> Build -> Build Artifacts...
-> storm:jar -> build
```
### 发布
```
发送到服务器: scp out/artifacts/storm_jar/storm.jar root@bigdata-pro01:/home/opt/jars/ 
发布: /# /home/opt/modules/apache-storm-1.2.3/bin/storm jar /home/opt/jars/storm.jar start.case6TridentTest.TridentOperTest wordCounter
```
### 删除（默认30s）
```
/home/opt/modules/apache-storm-1.2.3/bin/storm kill fileTopo
或者直接通过UI界面kill
```
## 客户端访问
run start/case6TridentTest/DrpcClientTest.java
```
[["2020-01-12",408],["2020-01-14",408],["2020-01-11",3200]]
[["2020-01-12",416],["2020-01-14",416],["2020-01-11",3264]]
[["2020-01-12",424],["2020-01-14",424],["2020-01-11",3328]]
[["2020-01-12",432],["2020-01-14",432],["2020-01-11",3392]]
``` 