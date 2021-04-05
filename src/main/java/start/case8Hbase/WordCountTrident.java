/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package start.case8Hbase;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapper;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseState;
import org.apache.storm.hbase.trident.state.HBaseStateFactory;
import org.apache.storm.hbase.trident.state.HBaseUpdater;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import start.case8Hbase.hbase.topology.WordCountValueMapper;

import java.util.HashMap;
import java.util.Map;

public class WordCountTrident {
    public static StormTopology buildTopology(String hbaseRoot) {
        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", 1),
                new Values("trident", 1),
                new Values("needs", 1),
                new Values("javadoc", 1)
        );
        spout.setCycle(true);

        TridentHBaseMapper tridentHBaseMapper = new SimpleTridentHBaseMapper()
                .withColumnFamily("info")
                .withColumnFields(new Fields("_word", "_count"))
                //.withCounterFields(new Fields("count"))
                .withRowKeyField("_word");

        HBaseValueMapper rowToStormValueMapper = new WordCountValueMapper();

        HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria();
        projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData("info", "word"));
        projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData("info", "count"));

        HBaseState.Options options = new HBaseState.Options()
                .withConfigKey(hbaseRoot)
                .withDurability(Durability.SYNC_WAL)
                .withMapper(tridentHBaseMapper)
                .withProjectionCriteria(projectionCriteria)
                .withRowToStormValueMapper(rowToStormValueMapper)
                .withTableName("WordCount");

        StateFactory factory = new HBaseStateFactory(options);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout)
                .each(new Fields("word", "count"), new BaseFunction() {
                    @Override
                    public void execute(TridentTuple tuple, TridentCollector collector) {
                        String _count = String.valueOf(tuple.getIntegerByField("count"));
                        String _word = String.valueOf(tuple.getStringByField("word"));

                        collector.emit(new Values(_word, _count));
                    }
                }, new Fields("_word", "_count"));
        stream.partitionPersist(factory, new Fields("_word", "_count"), new HBaseUpdater(), new Fields());

        return topology.build();
    }

    public static void main(String[] args) {
        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        Map<String, Object> confMap = new HashMap<>();
        conf.put("hbase", confMap);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCounter", conf, buildTopology("hbase"));
    }

}
