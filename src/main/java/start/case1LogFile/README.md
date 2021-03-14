# 案例说明
```
1. 使用GetData程序自动生成日志文件，保存到 /home/opt/datas/track.log 文件。
2. ReadFileSpout程序创建spout，实时监控日志文件，逐行发布，字段命名为“log”。
3. 开发FileBolt程序，创建Bolt程序，监控Spout，读取“log”字段，统计日志行数以“num”字段发送。
4. 开发PrintBolt程序创建Bolt，读取Bolt发送的第一个Integer，打印结果。
```
