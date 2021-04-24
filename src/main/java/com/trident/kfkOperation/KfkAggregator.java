package com.trident.kfkOperation;

import clojure.lang.Numbers;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Created by Administrator on 2018/6/21.
 */
public class KfkAggregator {

    public static class shopRanking_CombinerAggre_Count implements CombinerAggregator<Long> {

        public Long init(TridentTuple tuple) {
            System.out.println("CombinerAggregator>>TridentTuple>"+tuple);
            System.out.println("CombinerAggregator>>TridentTuple >>>initValue>"+Long.valueOf(tuple.getString(1)));

            return Long.valueOf(tuple.getString(1));
        }

        public Long combine(Long val1, Long val2) {
            System.out.println("CombinerAggregator>>combine>"+val1+":"+val2);
            long re = val1 + val2;
            System.out.println("CombinerAggregator>>>"+re);
            return re;
        }

        public Long zero() {
            return Long.valueOf(1);
        }

    }

    public static class shopRanking_CombinerAggre_Sum implements CombinerAggregator<Number> {

        @Override
        public Number init(TridentTuple tuple) {
            return  Numbers.num(Long.valueOf((String) tuple.getValue(1)));
        }

        @Override
        public Number combine(Number val1, Number val2) {
            return Numbers.add(val1, val2);
        }

        @Override
        public Number zero() {
            return 0;
        }

    }

    public static class shopRanking_CombinerAggre_Sum2 implements CombinerAggregator<Number> {

        @Override
        public Number init(TridentTuple tuple) {
            return  Numbers.num(Long.valueOf((String) tuple.getValue(1)));
        }

        @Override
        public Number combine(Number val1, Number val2) {
            return Numbers.add(val1, val2);
        }

        @Override
        public Number zero() {
            return 0;
        }

    }


    public static class allAmt_CombinerAggre_Sum implements CombinerAggregator<Number> {

        @Override
        public Number init(TridentTuple tuple) {
            return  Numbers.num(Long.valueOf((String) tuple.getValue(0)));
        }

        @Override
        public Number combine(Number val1, Number val2) {
            return Numbers.add(val1, val2);
        }

        @Override
        public Number zero() {
            return 0;
        }

    }


}
