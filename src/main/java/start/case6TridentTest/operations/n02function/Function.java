package start.case6TridentTest.operations.n02function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class Function {
    public static class MyFunction extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String date = tuple.getStringByField("date");
            date = date.substring(0, 10);
            collector.emit(new Values(date));
        }
    }
}
