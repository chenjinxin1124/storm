/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cjx.com.trident.SpoutDatas;


import cjx.com.trident.kfkConfig.KfkTridentConfig;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

public class MainOrderProducer extends Thread
{
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public MainOrderProducer(String topic)
    {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "bigdata-pro01:9092");
        // Use random partitioner. Don't need the key type. Just set it to Integer.
        // The message is of type String.
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }

    public void run() {

        Random random = new Random();
          String[] shop_id = {"115974463","102781795","130175431","118332811","116372249","61287947","107017127","69971708","5993647","35939500"};
       // String[] shop_id = {"115974463","102781795"};

        Integer[] shop_amt = { 199, 20, 150,60, 80,19,24,50,160, 380,49,14, 80,12,9,1,6,122,156,189};


        for (int i = 0; i < 50000; i++) {
            {

                String message = shop_id[random.nextInt(10)] + "\t" + shop_amt[random.nextInt(20)];
                //String message = shop_id[random.nextInt(2)] + "\t" + shop_amt[random.nextInt(20)];

                System.out.println("message:"+message);
                producer.send(new KeyedMessage<Integer, String>(topic, message));


                try {
                    Thread.sleep(1000);
                }catch (Exception e ){
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args)
    {
        MainOrderProducer producerThread = new MainOrderProducer(KfkTridentConfig.topic_name);
        producerThread.start();
    }

}
