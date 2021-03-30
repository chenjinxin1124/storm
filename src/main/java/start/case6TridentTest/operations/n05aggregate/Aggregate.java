package start.case6TridentTest.operations.n05aggregate;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

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

    public static class CombinerAggre implements CombinerAggregator{
        @Override
        public Object init(TridentTuple tuple) {
            long amt = tuple.getIntegerByField("amt");
            String date = tuple.getStringByField("_date");
            System.out.println(date + ": " + amt);
            return amt;
        }

        @Override
        public Object combine(Object val1, Object val2) {
            return (long)val1 + (long)val2;
        }

        @Override
        public Object zero() {
            return 0L;
        }
    }
}
