/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trident.kfkOperation;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.Assembly;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;


/**
 *
 * An {@link Assembly} implementation
 *
 */
public class KfkFirstN implements Assembly {

    Aggregator _agg;

    public KfkFirstN(int n, String sortField) {
        this(n, sortField, false);
    }

    public KfkFirstN(int n, String sortField, boolean reverse) {
        if(sortField!=null) {
            _agg = new FirstNSortedAgg(n, sortField, reverse);
        }
    }

    @Override
    public Stream apply(Stream input) {
        Fields outputFields = input.getOutputFields();
        return input.partitionAggregate(outputFields, _agg, outputFields)
                .global()
                .partitionAggregate(outputFields, _agg, outputFields);
    }

    public static class FirstNSortedAgg extends BaseAggregator<PriorityQueue> {

        int _n;
        String _sortField;
        boolean _reverse;

        class EmitValue{
            String userid;
            long amt;
        }
        public FirstNSortedAgg(int n, String sortField, boolean reverse) {
            _n = n;
            _sortField = sortField;
            _reverse = reverse;
        }

        @Override
        public PriorityQueue init(Object batchId, TridentCollector collector) {

            System.out.println();
            return new PriorityQueue(_n, new Comparator<Map>() {
                @Override
                public int compare(Map t1, Map t2) {
                    System.out.println(t1);
                    System.out.println(t2);


                    long t1_value = Long.valueOf((String) t1.get("_cAmt"));
                    long t2_value = Long.valueOf((String) t2.get("_cAmt"));

                    Comparable c1 = (Comparable) t1_value;
                    Comparable c2 = (Comparable) t2_value;
                    int ret = c1.compareTo(c2);
                    if(_reverse) ret *= -1;
                    return ret;
                }
            });
        }

        @Override
        public void aggregate(PriorityQueue state, TridentTuple tuple, TridentCollector collector) {

            //System.out.println("PriorityQueue"+tuple.getValue(2));

            //PriorityQueue 数值：
            // [[[_cAmt, 195910], [_userid, caojinbo_0]],
            //  [[_cAmt, 202010], [_userid, caojinbo_1]],
            // [[_cAmt, 199410], [_userid, caojinbo_2]],
            // [[_cAmt, 197240], [_userid, caojinbo_3]],
            // [[_cAmt, 199650], [_userid, caojinbo_4]],
            // [[_cAmt, 199470], [_userid, caojinbo_5]],
            // [[_cAmt, 191720], [_userid, caojinbo_6]],
            // [[_cAmt, 201430], [_userid, caojinbo_7]]]

           try {
             //  List<List<Values>> list = (List) tuple.getValue(2);

  System.out.println("**********aggregate-tuple"+tuple);
              // for (List<Values> values : list) {
               //if(tuple.size() !=2) return;
               Map map = (Map)tuple.getValue(2);
                   state.add(map);
             //  }
           }catch (Exception e ){
               e.printStackTrace();
           }

        }

        @Override
        public void complete(PriorityQueue val, TridentCollector collector) {
            System.out.println("complete"+val);
            int total = val.size();
            for(int i=0; i<_n && i < total; i++) {
                Map t = (Map) val.remove();
                collector.emit(new Values(t));
            }

            //System.out.println("complete"+val);
        }
    }
}
