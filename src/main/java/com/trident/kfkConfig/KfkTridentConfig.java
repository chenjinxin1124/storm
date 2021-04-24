package com.trident.kfkConfig;

import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;

/**
 * Created by Administrator on 2018/6/21.
 */
public class KfkTridentConfig {


    public static String topic_name = "hbaseTest";
    public static TridentKafkaConfig getKafkaConfig(){

        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(new ZkHosts(
//                "bigdata-pro03:2181," +
//                        "bigdata-pro02:2181," +
                        "bigdata-pro01:2181"), "hbaseTest");

        //定义了output的字段名称
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        return kafkaConfig;
    }


}
