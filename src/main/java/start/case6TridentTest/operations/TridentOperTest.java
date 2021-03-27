package start.case6TridentTest.operations;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.*;
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


    static Integer[] amt = {10, 20, 30, 60, 58, 77, 125};
    static String[] date = {"2017-03-21 17:33:40", "2017-03-22 17:33:40", "2017-03-25 17:33:40", "2017-03-28 17:33:40"};
    static String[] city = {"1", "2", "3", "3"};
    static String[] product = {"221", "335", "115", "44"};


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
            System.out.println("原数据: [" + _date + " : " + amt + "]");
            collector.emit(new Values(_date));

        }
    }

    private static class PartionFunction extends BaseFunction {

        int parNum;
        int parIndex;

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            parNum = context.numPartitions();
            parIndex = context.getPartitionIndex();

            super.prepare(conf, context);
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {

            //String _date = tuple.getStringByField("_date");
            long amt = tuple.getLongByField("_amt");

            //_date = _date.substring(0,10);
            System.out.println("parNum***= " + parNum + "parIndex= " + parIndex + "  [" + amt + "]");
            //collector.emit(new Values(_date));

        }
    }

    private static class OperFilter extends BaseFilter {
        @Override
        public boolean isKeep(TridentTuple tuple) {

            int _amt = tuple.getIntegerByField("amt");
            return (_amt < 30) ? false : true;

        }
    }

    private static class PrintFilter extends BaseFilter {
        @Override
        public boolean isKeep(TridentTuple tuple) {

            System.out.println("result>>>>:   " + tuple);


            return false;

        }
    }

    /**
     * ReducerAggregator
     */
    private static class MyReduceAgrre implements ReducerAggregator {
        @Override
        public Object init() {
            return 0L;
        }

        @Override
        public Object reduce(Object curr, TridentTuple tuple) {
            int amt = tuple.getIntegerByField("amt");
            System.out.println(curr + ":" + amt);
            return (long) curr + 1;
        }
    }

    private static class MyCominberAgrre implements CombinerAggregator {
        @Override
        public Object init(TridentTuple tuple) {
            long amt = tuple.getIntegerByField("amt");
            String _date = tuple.getStringByField("_date");

            return amt;
        }

        @Override
        public Object combine(Object val1, Object val2) {
            return (long) val1 + (long) val2;
        }

        @Override
        public Object zero() {
            return 0L;
        }
    }

    private static class MyCominberAgrreCount implements CombinerAggregator {
        @Override
        public Object init(TridentTuple tuple) {
            long amt = tuple.getIntegerByField("amt");
            String _date = tuple.getStringByField("_date");

            return 1L;
        }

        @Override
        public Object combine(Object val1, Object val2) {
            return (long) val1 + (long) val2;
        }

        @Override
        public Object zero() {
            return 0L;
        }
    }

    public static class MyAgg extends BaseAggregator<MyAgg.CountState> {

        int parNum;
        int parIndex;

        static class CountState {
            long count = 0;
            long amtSum = 0;
            Object _batchId;
            String date;

            CountState(Object batchId) {
                this._batchId = batchId;
            }

        }

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            parNum = context.numPartitions();
            parIndex = context.getPartitionIndex();
        }


        public CountState init(Object batchId, TridentCollector collector) {
            return new CountState(batchId);
        }

        public void aggregate(CountState state, TridentTuple tuple, TridentCollector collector) {
            int amt = tuple.getIntegerByField("amt");
            String _date = tuple.getStringByField("_date");

            state.date = _date;
            state.amtSum += amt;
        }

        public void complete(CountState state, TridentCollector collector) {
            System.out.println("date=" + state.date + "     parNum >>=" + parNum + "    batchId=" + state._batchId + "  :   sum = " + state.amtSum);
            collector.emit(new Values(state.amtSum));
        }
    }


    public static StormTopology buildTopology(LocalDRPC drpc) {

        Fields fields = new Fields("date", "amt", "city", "product");
        Random random = new Random();
        FixedBatchSpout spout1 = new FixedBatchSpout(fields, 6,
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)]),
                new Values(date[random.nextInt(4)], amt[random.nextInt(7)], city[random.nextInt(4)], product[random.nextInt(4)])
        );
        spout1.setCycle(true);

        //select date ,count(1) amt_count,sum(amt) amt_sum from t1 where 1=1 group by date
        TridentTopology topology = new TridentTopology();

        TridentState state = topology.newStream("spout12", spout1).shuffle().parallelismHint(5)
                .each(new Fields("date", "amt", "city", "product"), new OperFunction(), new Fields("_date")) //date","amt","city","product","_date"
                .project(new Fields("_date", "amt"))
                .groupBy(new Fields("_date"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("_date", "amt"), new MyCominberAgrre(), new Fields("_amt"));

        //.partitionBy(new Fields("_date"))


        //.aggregate(new Fields("_date","amt"),new MyAgg(),new Fields("_amt")).parallelismHint(10);


        topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("_date"))
                .groupBy(new Fields("_date"))
                .stateQuery(state, new Fields("_date"), new MapGet(), new Fields("amt_sum"))//args,_date,amt_sum
                .each(new Fields("amt_sum"), new FilterNull())
                .project(new Fields("_date", "amt_sum"))
                .applyAssembly(new FirstN(3, "amt_sum", false));
        //.each(new Fields("_date","amt_sum"),new PrintFilter());

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
            for (int i = 0; i < 10000; i++) {
                System.out.println("DRPC RESULT: " + drpc.execute("words", "2017-03-21 2017-03-22 2017-03-25 2017-03-28"));
                Thread.sleep(1000);
            }
        }

    }
}
