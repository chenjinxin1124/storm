package cjx.com.trident.kfkTridentJDBC;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Created by Administrator on 2018/6/16.
 */
public class KfkJdbcStateFactory implements StateFactory {
    private KfkJdbcState.Options options;

    public KfkJdbcStateFactory(KfkJdbcState.Options options) {
        this.options = options;
    }

    public State makeState(Map map, IMetricsContext iMetricsContext, int partitionIndex, int numPartitions) {
        KfkJdbcState state = new KfkJdbcState(map, partitionIndex, numPartitions, this.options);
        state.prepare();
        return state;
    }
}
