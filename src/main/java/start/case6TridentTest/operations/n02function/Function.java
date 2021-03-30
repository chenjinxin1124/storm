package start.case6TridentTest.operations.n02function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class Function {
    public static class MyFunction extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String date = tuple.getStringByField("date");
            date = date.substring(0, 10);
            collector.emit(new Values(date));
        }
    }

    public static class PartionFunction extends BaseFunction {

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
            String date = tuple.getStringByField("_date");
            Integer amt = tuple.getIntegerByField("amt");
            date = date.substring(0, 10);
            StringBuilder sb = new StringBuilder(" ");
            for (int i = 0; i < parIndex; i++) {
                sb.append("================");
            }
            sb.append("=>");
            System.out.println("parNum = " + parNum + ", parIndex = " + parIndex + sb.toString() + " [ " + amt + ", " + date + " ]");
        }
    }

    public static class OperFunction extends BaseFunction {
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
            System.out.println("原数据: [" + "numPar => " + numPar + " --> " + _date + " : " + amt + "]");
            collector.emit(new Values(_date));

        }
    }
}
