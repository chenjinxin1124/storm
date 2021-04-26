package cjx.com.trident.kfkOperation;

import cjx.com.trident.kfkTridentUtil.KfkUtil;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Administrator on 2018/6/21.
 */
public class KfkFunctions {


    public static class JDBCSplitPrint extends BaseFunction {
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            tridentCollector.emit(new Values(tridentTuple.getString(0),String.valueOf(tridentTuple.getLong(
                    1))));

        }
    }

    public static class SpoutSplitPrint extends BaseFunction {
        @Override
        public void prepare(Map conf, TridentOperationContext context) {
           System.out.println("SpoutSplitPrint partitionNum:"+context.numPartitions() +">>>>>>partitionIndex:"+context.getPartitionIndex());
        }

        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String[] strs = tridentTuple.getString(0).split("\t");
            tridentCollector.emit(new Values(strs[0],strs[1]));

        }
    }

    public static class ForCreateAllKey extends BaseFunction {
        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            System.out.println("SpoutSplitPrint partitionNum:"+context.numPartitions() +">>>>>>partitionIndex:"+context.getPartitionIndex());
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String allValue = String.valueOf(tuple.getValue(1));
            collector.emit(new Values("kfk-all",allValue));
        }
    }


    public static class ForCreateAllKey_intoTime extends BaseFunction {
        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            System.out.println("SpoutSplitPrint partitionNum:"+context.numPartitions() +">>>>>>partitionIndex:"+context.getPartitionIndex());
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {

            String currTime = KfkUtil.getCurrTime();
            String amtValue = String.valueOf(tuple.getLong(0));
            collector.emit(new Values(currTime,amtValue));
        }
    }



}
