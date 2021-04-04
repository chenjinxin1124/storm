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

package start.case7Kafka.Demo2;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaUpdater;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Properties;

public class KafkaProducerTopology {


    public static StormTopology newTridentTopology(String brokerUrl, String topicName) {
        Fields fields = new Fields("word", "count");
        FixedBatchSpout spout = new FixedBatchSpout(fields, 4,
                new Values("storm", "1"),
                new Values("trident", "1"),
                new Values("needs", "1"),
                new Values("javadoc", "1")
        );
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);

//set producer properties.
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerUrl);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        TridentKafkaStateFactory stateFactory = new TridentKafkaStateFactory()
                .withProducerProperties(props)
                .withKafkaTopicSelector(new DefaultTopicSelector(topicName))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("word", "count"));
        stream.partitionPersist(stateFactory, fields, new TridentKafkaUpdater(), new Fields());


        return topology.build();
    }

    /**
     * @param brokerUrl Kafka broker URL
     * @param topicName Topic to which publish sentences
     * @return A Storm topology that produces random sentences using {@link RandomSentenceSpout} and uses a {@link KafkaBolt} to
     * publish the sentences to the kafka topic specified
     */
//    public static StormTopology newTopology(String brokerUrl, String topicName) {
//        final TopologyBuilder builder = new TopologyBuilder();
//        builder.setSpout("spout", new RandomSentenceSpout.TimeStamped(""), 2);
//
//        /* The output field of the RandomSentenceSpout ("word") is provided as the boltMessageField
//          so that this gets written out as the message in the kafka topic. */
//        final KafkaBolt<String, String> bolt = new KafkaBolt<String, String>()
//                .withProducerProperties(newProps(brokerUrl, topicName))
//                .withTopicSelector(new DefaultTopicSelector(topicName))
//                .<String, String>withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<String,String>("key", "word"));
//
//        builder.setBolt("forwardToKafka", bolt, 1).shuffleGrouping("spout");
//
//        return builder.createTopology();
//    }

    /**
     * @return the Storm config for the topology that publishes sentences to kafka using a kafka bolt.
     */
//    private static Properties newProps(final String brokerUrl, final String topicName) {
//        return new Properties() {{
//            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
//            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//            put(ProducerConfig.CLIENT_ID_CONFIG, topicName);
//        }};
//    }
}


