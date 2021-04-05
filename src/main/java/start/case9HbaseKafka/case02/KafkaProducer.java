/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package start.case9HbaseKafka.case02;


import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

public class KafkaProducer extends Thread {
    private final kafka.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public KafkaProducer(String topic) {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "bigdata-pro01:9092,bigdata-pro02:9092,bigdata-pro03:9092");
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }

    static Integer[] amt = {1, 2, 4, 8, 16, 32, 64};
    static String[] date = {"2020-01-11 12:23:34", "2020-01-12 12:23:34", "2020-01-13 12:23:34", "2020-01-14 12:23:34"};
    static String[] city = {"beijing", "shanghai", "guangzhou", "shenzhen"};
    static String[] product = {"128", "256", "512", "1024"};


    public void run() {
        Random random = new Random();
        for (int i = 0; i < 1500000000; i++) {
            {
                StringBuilder str = new StringBuilder();
                str.append(date[random.nextInt(4)]).append(",");
                str.append(amt[random.nextInt(7)]).append(",");
                str.append(city[random.nextInt(4)]).append(",");
                str.append(product[random.nextInt(4)]);

                producer.send(new KeyedMessage<Integer, String>(topic, str.toString()));
                System.out.println("message:" + str.toString());
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        KafkaProducer producerThread = new KafkaProducer("test2");
        producerThread.start();
    }

}
