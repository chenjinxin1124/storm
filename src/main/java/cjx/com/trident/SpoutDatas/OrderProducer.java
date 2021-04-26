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


import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

public class OrderProducer extends Thread
{
  private final kafka.javaapi.producer.Producer<Integer, String> producer;
  private final String topic;
  private final Properties props = new Properties();

  public OrderProducer(String topic)
  {
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", "bigdata-pro01:9092");
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
    this.topic = topic;
  }

  public void run() {
    int messageNo = 1;

    Random random = new Random();
    // order_id,order_amt,create_time,area_id,user_id

    Integer[] order_amt = { 60, 20, 50,60, 80};


    String[] province_id = { "1","2","3","4","5","6","7","8" };

    //String[] user_id = { "1123","2132","334","4456","567","678","798","856" };
    String[] user_id = new String[10];
    for(int i=0;i < 10;i++){
          user_id[i] = "caojinbo_"+i;
    }

    for (int i = 0; i < 1500000000; i++) {
      {

        String message = order_amt[random.nextInt(5)] + "\t" + province_id[random.nextInt(8)] + "\t" + user_id[random.nextInt(8)];
        producer.send(new KeyedMessage<Integer, String>(topic, message));

        System.out.println("message:"+message);
        try {
          Thread.sleep(1);
        }catch (Exception e ){
          e.printStackTrace();
        }
      }
    }
  }

  public static void main(String[] args)
  {
    OrderProducer producerThread = new OrderProducer("hbaseTest");
    producerThread.start();
  }

}
