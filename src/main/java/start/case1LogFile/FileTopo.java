package start.case1LogFile;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class FileTopo {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new ReadFileSpout(), 1);// parallelism_hint: 并发度
        builder.setBolt("fileBolt", new FileBolt(), 1).shuffleGrouping("spout");
        builder.setBolt("printBolt", new PrintBolt(), 1).shuffleGrouping("fileBolt");

        Config config = new Config();
        config.setDebug(true);

        if (args != null && args.length > 0) {
            config.setNumWorkers(3);// worker数必须大于等于组件数
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("filePrint", config, builder.createTopology());
        }
    }
}
