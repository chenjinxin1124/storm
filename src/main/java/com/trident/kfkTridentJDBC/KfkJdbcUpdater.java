package com.trident.kfkTridentJDBC;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Created by Administrator on 2018/6/16.
 */
public class KfkJdbcUpdater extends BaseStateUpdater<KfkJdbcState> {
    public KfkJdbcUpdater() {
    }


    public void updateState(KfkJdbcState jdbcState, List<TridentTuple> tuples, TridentCollector collector) {
        try {
            jdbcState.updateState(tuples, collector);
            //Thread.sleep(10000);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}

