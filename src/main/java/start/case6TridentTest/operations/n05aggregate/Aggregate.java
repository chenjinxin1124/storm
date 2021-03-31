package start.case6TridentTest.operations.n05aggregate;

import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class Aggregate {
    public static class ReducerAggre implements ReducerAggregator {
        @Override
        public Object init() {
            return 0L;
        }

        @Override
        public Object reduce(Object curr, TridentTuple tuple) {
            Integer amt = tuple.getIntegerByField("amt");
            System.out.println(curr + ": " + amt);
            return (long) curr + amt;
        }
    }

    public static class CombinerAggre implements CombinerAggregator {
        @Override
        public Object init(TridentTuple tuple) {
            long amt = tuple.getIntegerByField("amt");
            String date = tuple.getStringByField("_date");
            System.out.println(date + ": " + amt);
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

    public static class CombinerAggreCount implements CombinerAggregator {
        @Override
        public Object init(TridentTuple tuple) {
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

    public static class BaseAgger extends BaseAggregator<BaseAgger.CountState> {
        int parNum;
        int parIndex;

        static class CountState {
            long count = 0;
            long amtSum = 0;
            Object _batchId;
            String date;

            CountState(Object _batchId) {
                this._batchId = _batchId;
            }
        }

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            parNum = context.numPartitions();
            parIndex = context.getPartitionIndex();
        }

        @Override
        public CountState init(Object batchId, TridentCollector collector) {
            return new CountState(batchId);
        }

        @Override
        public void aggregate(CountState val, TridentTuple tuple, TridentCollector collector) {
            Integer amt = tuple.getIntegerByField("amt");
            val.date = tuple.getStringByField("_date");
            val.amtSum += amt;
        }

        @Override
        public void complete(CountState val, TridentCollector collector) {
            System.out.println("date = " + val.date + "\tparNum => " + parNum + "\tbatchId = " + val._batchId + "\tsum = " + val.amtSum);
            collector.emit(new Values(val.amtSum));
        }

    }
}
