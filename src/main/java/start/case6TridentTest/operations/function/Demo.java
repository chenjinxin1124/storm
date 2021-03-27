package start.case6TridentTest.operations.function;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import start.case6TridentTest.operations.Datas;
import start.case6TridentTest.operations.filter.Filter;

public class Demo {

    public static StormTopology buildTopology() {

        FixedBatchSpout spout = new Datas().getSpout();
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout)
                .each(new Fields("date", "amt", "city", "product"), new Function.MyFunction(), new Fields("_date"))// 此时的Tuple有"date", "amt", "city", "product", "_date"5列数据
                .each(new Fields("_date", "amt", "city", "product"), new Filter.PrintFilter());// 这里只取了Tuple的"_date", "amt", "city", "product"
        return topology.build();
    }

    public static void main(String[] args) {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setDebug(false);
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology());
        }
    }
}
