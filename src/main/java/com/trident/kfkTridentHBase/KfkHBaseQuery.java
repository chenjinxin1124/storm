package com.trident.kfkTridentHBase;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.omg.CORBA.Object;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/6/13.
 */

public class KfkHBaseQuery extends BaseQueryFunction<KfkHBaseState, List<Values>> {

    @Override
    public List<List<Values>> batchRetrieve(KfkHBaseState hBaseState, List<TridentTuple> tridentTuples) {
        return hBaseState.batchRetrieve(tridentTuples);
    }

    @Override
    public void execute(TridentTuple tuples, List<Values> values, TridentCollector tridentCollector) {
        Map<String,Object> val = new HashMap<String,Object>();
        for (Values value : values) {
            tridentCollector.emit(value);
        }
    }
}