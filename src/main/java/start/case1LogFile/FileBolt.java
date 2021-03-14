package start.case1LogFile;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class FileBolt implements IRichBolt {

    OutputCollector collector = null;
    int num = 0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {// 入口方法
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {// 核心执行方法
        String log = tuple.getStringByField("log");// 获取数据
        num++;
        this.collector.emit(new Values(num));// 发送数据
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("num"));// 定义发送数据的字段
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
