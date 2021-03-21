package start.case5AckAndFail;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class AckAndFailTopo {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("AckAndFailSpout", new AckAndFailSpout(), 1);
        builder.setBolt("AckBolt", new AckBolt(), 1).shuffleGrouping("AckAndFailSpout");
        builder.setBolt("AckBolt2", new AckBolt2(), 1).shuffleGrouping("AckBolt");

        Config config = new Config();
        config.setDebug(false);
        //		conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0); 设置不支持 ack fail

        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("AckAndFailTopo", config, builder.createTopology());
        }

    }
}
