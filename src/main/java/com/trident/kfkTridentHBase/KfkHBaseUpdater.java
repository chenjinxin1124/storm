package com.trident.kfkTridentHBase;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Created by Administrator on 2018/6/13.
 */
public class KfkHBaseUpdater extends BaseStateUpdater<KfkHBaseState> {

    @Override
    public void updateState(KfkHBaseState hBaseState, List<TridentTuple> tuples, TridentCollector collector) {
        hBaseState.updateState(tuples, collector);
    }
}

