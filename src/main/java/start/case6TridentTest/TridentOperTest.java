package start.case6TridentTest;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class TridentOperTest {

    static Integer[] amt = {1, 2, 4, 8, 16, 32, 64};
    static String[] date = {"2020-01-11 12:23:34", "2020-01-12 12:23:34", "2020-01-13 12:23:34", "2020-01-14 12:23:34"};
    static String[] city = {"beijing", "shanghai", "guangzhou", "shenzhen"};
    static String[] product = {"128", "256", "512", "1024"};

    private static class OperFunction extends BaseFunction {
        int numPar;

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            numPar = context.numPartitions();
            super.prepare(conf, context);
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String _date = tuple.getStringByField("date");
            int amt = tuple.getIntegerByField("amt");

            _date = _date.substring(0, 10);
            //System.out.println("原数据: [" + date + " : " + amt + "]");
            collector.emit(new Values(_date));
        }
    }

    private static class MyCominberAgrre implements CombinerAggregator {
        @Override
        public Object init(TridentTuple tuple) {
            Integer amt = tuple.getIntegerByField("amt");
            String _date = tuple.getStringByField("_date");
            return amt;
        }

        @Override
        public Object combine(Object val1, Object val2) {
            return (Integer) val1 + (Integer) val2;
        }

        @Override
        public Object zero() {
            return 0;
        }
    }

    public static StormTopology buildTopology(LocalDRPC drpc) {
        Fields fields = new Fields("date", "amt", "city", "product");
        Random random = new Random();
        FixedBatchSpout spout = new FixedBatchSpout(fields, 6,
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)])
        );
        spout.setCycle(true);

        //select date ,count(1) amt_count,sum(amt) amt_sum from t1 where 1=1 group by date
        TridentTopology topology = new TridentTopology();

        TridentState state = topology.newStream("spout", spout)
                .shuffle()
                .parallelismHint(5)
                .each(new Fields("date", "amt", "city", "product"),
                        new OperFunction(),
                        new Fields("_date"))
                .project(new Fields("_date", "amt"))
                .groupBy(new Fields("_date"))
                .persistentAggregate(new MemoryMapState.Factory(),
                        new Fields("_date", "amt"),
                        new MyCominberAgrre(),
                        new Fields("_amt"));

        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"),
                        new Split(),
                        new Fields("_date"))
                .groupBy(new Fields("_date"))
                .stateQuery(state,
                        new Fields("_date"),
                        new MapGet(),
                        new Fields("amt_sum"))
                .each(new Fields("amt_sum"),
                        new FilterNull())
                .project(new Fields("_date", "amt_sum"))
                .applyAssembly(new FirstN(3,
                        "amt_sum",
                        false));

        return topology.build();
    }

    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        config.setMaxSpoutPending(20);
        config.setDebug(false);
        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", config, buildTopology(drpc));
            for (int i = 0; i < 100; ++i) {
                System.out.println("DRPC RESULT: " + drpc.execute("words", "2020-01-11 2020-01-12 2020-01-13 2020-01-14"));
                Thread.sleep(1000);
            }
        } else {
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], config, buildTopology(null));
        }
    }
}
