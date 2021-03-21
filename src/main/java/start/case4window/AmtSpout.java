package start.case4window;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class AmtSpout implements IRichSpout {

    Integer[] amt = {10, 20, 40, 80};
    String[] date = {"2021-03-20", "2021-03-21", "2021-03-22", "2021-03-23"};
    String[] city = {"beijing", "shanghai", "guangzhou", "shenzhen"};
    String[] product = {"yi", "shi", "zhu", "xing"};

    SpoutOutputCollector _collector = null;
    Random random = new Random();

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
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
    public void nextTuple() {
        int id = random.nextInt(1000);
        int _amt = amt[random.nextInt(4)];
        String _date = date[random.nextInt(4)];
        String _city = city[random.nextInt(4)];
        String _product = product[random.nextInt(4)];
        _collector.emit(new Values(String.valueOf(id), _date, String.valueOf(_amt), _city, _product), "msg");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "date", "amt", "city", "product"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
