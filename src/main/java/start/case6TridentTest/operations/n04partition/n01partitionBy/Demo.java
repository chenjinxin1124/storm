package start.case6TridentTest.operations.n04partition.n01partitionBy;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import start.case6TridentTest.operations.Datas;
import start.case6TridentTest.operations.n01filter.Filter;
import start.case6TridentTest.operations.n02function.Function;

public class Demo {

    public static StormTopology buildTopology() {

        FixedBatchSpout spout = new Datas().getSpout();
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout)
                .each(new Fields("date", "amt", "city", "product"), new Function.MyFunction(), new Fields("_date"))// 此时的Tuple有"date", "amt", "city", "product", "_date"5列数据
                .project(new Fields("_date", "amt"))// 投影操作：只保留 _date,amt 两个字段的数据，相当于select选取字段
                .partitionBy(new Fields("_date"))// 按照 _date 字段分区
                .each(new Fields("amt", "_date"), new Function.PartionFunction(), new Fields())// 打印分区数，分区索引，tuple值
                .parallelismHint(4)// 设置并行度为4，为了让分区数操作可以分出来
        ;
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
