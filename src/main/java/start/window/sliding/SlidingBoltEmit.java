package start.window.sliding;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;

public class SlidingBoltEmit extends BaseWindowedBolt {

    OutputCollector _collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        super.prepare(stormConf, context, collector);
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        int amt = 0;
        List<Tuple> windowNew = inputWindow.getNew();
        List<Tuple> windowExpired = inputWindow.getExpired();

        System.out.println("windowNew");
        for (Tuple tuple : windowNew) {
            int a = Integer.parseInt(tuple.getStringByField("amt"));
            System.out.println(tuple.getStringByField("id") + ": " + a);
            amt += a;
        }

        System.out.println("windowExpired");
        for (Tuple tuple : windowExpired) {
            int a = Integer.parseInt(tuple.getStringByField("amt"));
            System.out.println(tuple.getStringByField("id") + ": " + a);
            amt -= a;
        }
        System.out.println("新增数量: "+amt);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("window_amt"));
        super.declareOutputFields(declarer);
    }
}
