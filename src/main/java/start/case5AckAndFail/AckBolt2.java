package start.case5AckAndFail;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class AckBolt2 implements IRichBolt {

    OutputCollector collector = null;
    TopologyContext context = null;

    //初始化，对应spout的open函数
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.context = context;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String date = input.getStringByField("date");
            double orderAmt = Double.parseDouble(input.getStringByField("orderAmt"));
            if (date.startsWith("3")) {
                throw new Exception("测试fail功能，当 date.startsWith(3) 时，AckBolt2 抛出错误。");
            }
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
