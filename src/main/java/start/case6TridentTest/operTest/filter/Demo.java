package start.case6TridentTest.operTest.filter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import start.case6TridentTest.operTest.Datas;

public class Demo {

    public static StormTopology buildTopology() {

        Datas datas = new Datas();
        Fields fields = new Fields("date", "amt", "city", "product");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 3,
                new Values(datas.getRandomDate(), datas.getRandomAmt(), datas.getRandomCity(), datas.getRandomProduct()),
                new Values(datas.getRandomDate(), datas.getRandomAmt(), datas.getRandomCity(), datas.getRandomProduct()),
                new Values(datas.getRandomDate(), datas.getRandomAmt(), datas.getRandomCity(), datas.getRandomProduct()),
                new Values(datas.getRandomDate(), datas.getRandomAmt(), datas.getRandomCity(), datas.getRandomProduct()),
                new Values(datas.getRandomDate(), datas.getRandomAmt(), datas.getRandomCity(), datas.getRandomProduct()),
                new Values(datas.getRandomDate(), datas.getRandomAmt(), datas.getRandomCity(), datas.getRandomProduct()),
                new Values(datas.getRandomDate(), datas.getRandomAmt(), datas.getRandomCity(), datas.getRandomProduct())
        );
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout)
                .each(new Fields("date", "amt", "city", "product"), new Filter.BiggerFilter(16))
                .each(new Fields("date", "amt", "city", "product"), new Filter.PrintFilter());
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
