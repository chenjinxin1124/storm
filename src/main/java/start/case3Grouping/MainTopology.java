package start.case3Grouping;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * select date,sum(amt) from t1 where 1=1 group by date;
 */
public class MainTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new AmtSpout());
        builder.setBolt("amtBolt", new AmtBolt(), 1).fieldsGrouping("spout", new Fields("date"));
        builder.setBolt("printBolt", new PrintBolt(), 2).shuffleGrouping("amtBolt");

        Config config = new Config();
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("amtTopo", config, builder.createTopology());
    }
}
