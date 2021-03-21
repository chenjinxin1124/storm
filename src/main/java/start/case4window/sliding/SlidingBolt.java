package start.case4window.sliding;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;

public class SlidingBolt extends BaseWindowedBolt {
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
}
