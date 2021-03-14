package start.case1LogFile;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Map;

public class ReadFileSpout implements IRichSpout {

    FileInputStream fis = null;
    InputStreamReader isr = null;
    BufferedReader br = null;
    String str = null;
    SpoutOutputCollector collector = null;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {// 入口方法
        try {
            fis = new FileInputStream("/home/opt/datas/track.log");
            isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);
            collector = spoutOutputCollector;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {// 核心执行方法
        try {
            while ((str = br.readLine()) != null) {
                collector.emit(new Values(str));// 发送数据
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log"));// 定义发送数据的字段
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
