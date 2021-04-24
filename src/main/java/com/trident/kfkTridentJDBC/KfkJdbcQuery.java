package com.trident.kfkTridentJDBC;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/6/19.
 */
public class KfkJdbcQuery extends BaseQueryFunction<KfkJdbcState, List<Values>> {
    public KfkJdbcQuery() {
    }

    public List<List<Values>> batchRetrieve(KfkJdbcState jdbcState, List<TridentTuple> tridentTuples) {

        return jdbcState.batchRetrieveAll(tridentTuples);
    }

    public void execute(TridentTuple tuples, List<Values> values, TridentCollector tridentCollector) {
        Iterator var4 = values.iterator();

        while(var4.hasNext()) {
            Map rowMap = (Map)var4.next();
            //Map value = (Map)list.get(0);
            tridentCollector.emit(new Values(rowMap));
        }

    }
}
