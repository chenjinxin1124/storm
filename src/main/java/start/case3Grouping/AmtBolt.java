package start.case3Grouping;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class AmtBolt implements IRichBolt {
    OutputCollector _collector = null;
    Map<String,Object>  amtMap = new HashMap<String,Object>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        String date = input.getStringByField("date");
        int amt = Integer.parseInt(input.getStringByField("amt"));

        if(amtMap.get(date) != null){
            amt += Integer.parseInt(String.valueOf(amtMap.get(date)));
        }
        amtMap.put(date,amt);

        _collector.emit(new Values(amtMap));
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("res"));
    }

    @Override
    public void cleanup() {

    }
}
