# 案例说明
```
1. 使用GetData程序自动生成日志文件，保存到 /home/opt/datas/track.log 文件。
2. ReadFileSpout程序创建spout，实时监控日志文件，逐行发布，字段命名为“log”。
3. 开发FileBolt程序，创建Bolt程序，监控Spout，读取“log”字段，统计日志行数以“num”字段发送。
4. 开发PrintBolt程序创建Bolt，读取Bolt发送的第一个Integer，打印结果。
```
# 打包发布删除
## 打包
```
Project Structure 
-> Project Setting -> Artifacts -> '+' -> JAR -> From modules with dependencies... 
-> Module: Storm, Main Class: FileTopo -> OK 
-> storm:jar -> Output Layout -> 删除'storm'comlile output以外的包
-> Build -> Build Artifacts...
-> storm:jar -> build
```
## 发布
```
启动zk服务
启动storm服务
发送到服务器: scp out/artifacts/storm_jar/storm.jar root@bigdata-pro01:/home/opt/jars/ 
发布: /home/opt/modules/apache-storm-1.2.3/bin/storm jar /home/opt/jars/storm.jar start.case1LogFile.FileTopo fileTopo
发送日志文件: scp /home/opt/datas/track.log root@bigdata-pro01:/home/opt/datas/
发送日志文件: scp /home/opt/datas/track.log root@bigdata-pro02:/home/opt/datas/
发送日志文件: scp /home/opt/datas/track.log root@bigdata-pro03:/home/opt/datas/
```
## 删除（默认30s）
```
/home/opt/modules/apache-storm-1.2.3/bin/storm kill fileTopo
或者直接通过UI界面kill
```
