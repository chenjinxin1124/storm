# 项目打包
## 打包
先删除：META-INF文件夹
```
Project Structure 
-> Project Setting -> Artifacts -> '+' -> JAR -> From modules with dependencies... 
-> Module: Storm, Main Class: OpMainTopology -> OK 
-> storm:jar -> Output Layout -> 删除'storm'comlile output以外的包
-> Build -> Build Artifacts...
-> storm:jar -> build
```
可以删除 storm:jar 中除了com以外的文件
## 发布(以 shop_topo 为名发布)
```
发送到服务器: scp out/artifacts/storm_jar/storm.jar root@bigdata-pro01:/home/opt/jars/ 
发布: /home/opt/modules/apache-storm-1.2.3/bin/storm jar /home/opt/jars/storm.jar com.trident.OpMainTopology shop_topo
```
# 运行
1. com.trident.SpoutDatas.MainOrderProducer
2. tomcat

