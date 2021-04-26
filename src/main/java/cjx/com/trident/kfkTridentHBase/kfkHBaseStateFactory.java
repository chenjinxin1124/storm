package cjx.com.trident.kfkTridentHBase;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Created by Administrator on 2018/6/13.
 */
public class kfkHBaseStateFactory implements StateFactory {

    private KfkHBaseState.Options options;

    public kfkHBaseStateFactory(KfkHBaseState.Options options) {
        this.options = options;
    }

    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int partitionIndex, int numPartitions) {
        KfkHBaseState state = new KfkHBaseState(map , partitionIndex, numPartitions, options);
        state.prepare();
        return state;
    }
}
