package start.case6TridentTest.operations.n11FirstN;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import start.case6TridentTest.operations.Datas;
import start.case6TridentTest.operations.n02function.Function;
import start.case6TridentTest.operations.n05aggregate.Aggregate;

public class DemoDRPC {

    public static StormTopology buildTopology(LocalDRPC drpc) {

        FixedBatchSpout spout = new Datas().getSpout();
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        // select date, count(1) count, sum(amt) amt_sum from t1 where 1=1 group by date. 累计
        TridentState state = topology.newStream("spout", spout)
                .shuffle()
                .parallelismHint(4)// 设置并行度为 4
                .each(new Fields("date", "amt", "city", "product"), new Function.OperFunction(), new Fields("_date"))// 此时的Tuple有"date", "amt", "city", "product", "_date"5列数据
                .project(new Fields("_date", "amt"))// 投影操作：只保留 _date,amt 两个字段的数据，相当于select选取字段
                .groupBy(new Fields("_date"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("_date", "amt"), new Aggregate.CombinerAggre(), new Fields("_amt"));

        topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("_date"))
                .groupBy(new Fields("_date"))
                .stateQuery(state, new Fields("_date"), new MapGet(), new Fields("amt_sum"))//args,_date,amt_sum
                .each(new Fields("amt_sum"), new FilterNull())
                .project(new Fields("_date", "amt_sum"))
                .applyAssembly(new FirstN(3, "amt_sum", false));
//                .each(new Fields("_date", "amt_sum"), new Filter.PrintFilter());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setDebug(false);
        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for (int i = 0; i < 10000; i++) {// 因为只拿了Top3，所以打印结果只有三组
                System.out.println("DRPC RESULT: " + drpc.execute("words", "2020-01-11 2020-01-12 2020-01-13 2020-01-14"));
                Thread.sleep(1000);
            }
        }

    }
}
