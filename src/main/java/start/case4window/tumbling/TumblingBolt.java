package start.case4window.tumbling;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;

public class TumblingBolt extends BaseWindowedBolt {
    @Override
    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuples = inputWindow.get();
        Double amt = 0.0;
        for (Tuple tuple : tuples) {
            Double a = Double.parseDouble(tuple.getStringByField("amt"));
            System.out.println(tuple.getStringByField("id") + ": " + a);
            amt += a;
        }
        System.out.println(amt);
    }
}
