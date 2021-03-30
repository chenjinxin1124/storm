package start.case6TridentTest.operations.n05aggregate;

import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class Aggregate {
    public static class ReduceAgregate implements ReducerAggregator {
        @Override
        public Object init() {
            return 0L;
        }

        @Override
        public Object reduce(Object curr, TridentTuple tuple) {
            Integer amt = tuple.getIntegerByField("amt");
            System.out.println(curr + ": " + amt);
            return (long) curr + 1;
        }
    }
}
