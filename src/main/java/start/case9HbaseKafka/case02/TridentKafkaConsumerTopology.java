/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package start.case9HbaseKafka.case02;

import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapMapper;
import org.apache.storm.hbase.trident.state.HBaseMapState;
import org.apache.storm.hbase.trident.state.HBaseQuery;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.spout.ITridentDataSource;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TridentKafkaConsumerTopology {
    protected static final Logger LOG = LoggerFactory.getLogger(TridentKafkaConsumerTopology.class);


    public static StormTopology newTopology(ITridentDataSource tridentSpout) {
        return newTopology(null, tridentSpout);
    }


    public static StormTopology newTopology(LocalDRPC drpc, ITridentDataSource tridentSpout) {

        final TridentTopology tridentTopology = new TridentTopology();
        addTridentState(tridentTopology, tridentSpout);
        return tridentTopology.build();
    }

    public static class MyFunction extends BaseFunction {
        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            super.prepare(conf, context);
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {

            String[] res = tuple.getStringByField("str").split(",");
            String date = res[0].substring(0, 10);
            String amt = res[1];
            System.out.println(" >>>>  " + date + " : " + amt);
            collector.emit(new Values(date, amt));
        }
    }

    private static class MySum implements CombinerAggregator {
        @Override
        public Object init(TridentTuple tuple) {
            long _amt = Long.parseLong(tuple.getStringByField("amt"));
            return _amt;
        }

        @Override
        public Object combine(Object val1, Object val2) {
            return (long) val1 + (long) val2;
        }

        @Override
        public Object zero() {
            return 0L;
        }
    }

    private static TridentState addTridentState(TridentTopology tridentTopology, ITridentDataSource tridentSpout) {
        final Stream spoutStream = tridentTopology.newStream("spout1", tridentSpout).parallelismHint(2);


        HBaseMapState.Options options = new HBaseMapState.Options();
        options.tableName = "storm";
        options.columnFamily = "info";
        options.mapMapper = new SimpleTridentHBaseMapMapper("count");

        TridentState tridentState =
                spoutStream.each(spoutStream.getOutputFields(), new Debug(true))
                        .each(new Fields("str"), new MyFunction(), new Fields("date", "amt"))
                        .groupBy(new Fields("date"))
                        .persistentAggregate(HBaseMapState.transactional(options), new Fields("date", "amt"), new MySum(), new Fields("_amt"));

        tridentTopology.newDRPCStream("test").stateQuery(tridentState, new HBaseQuery(), new Fields("ss"));


        tridentState.newValuesStream()
                .each(new Fields("date", "_amt"), new BaseFilter() {
                    @Override
                    public boolean isKeep(TridentTuple tuple) {
                        System.out.println("result >>>>  " + tuple);
                        return false;
                    }
                });

        return tridentState;
    }
}
