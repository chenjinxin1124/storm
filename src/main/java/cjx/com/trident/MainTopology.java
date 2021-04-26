package cjx.com.trident;

import cjx.com.trident.kfkConfig.KfkTridentConfig;
import cjx.com.trident.kfkConfig.TableAndColumn;
import cjx.com.trident.kfkDRPC.KfkLocalDrpc;
import cjx.com.trident.kfkDRPC.LocalSubmitter;
import cjx.com.trident.kfkOperation.KfkAggregator;
import cjx.com.trident.kfkOperation.KfkFunctions;
import cjx.com.trident.kfkTridentHBase.KfkHBaseMapState;
import cjx.com.trident.kfkTridentHBase.KfkHBaseQuery;
import cjx.com.trident.kfkTridentHBase.KfkHBaseUpdater;
import cjx.com.trident.kfkTridentHBase.kfkHBaseStateFactory;
import cjx.com.trident.kfkTridentJDBC.KfkJdbcQuery;
import cjx.com.trident.kfkTridentJDBC.KfkJdbcUpdater;
import cjx.com.trident.kfkTridentUtil.KfkStateOptions;
import org.apache.commons.collections.map.HashedMap;
import org.apache.storm.Config;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2018/5/20.
 */
public class MainTopology {

    private static final Logger LOG = LoggerFactory.getLogger(MainTopology.class);

    /**
     * 构建StormTopology
     *
     * @return
     */
    public static StormTopology buildTopology(LocalDRPC drpc) {


        TridentTopology topology = new TridentTopology();

        /**>>>>>>店铺销售额计算并排名**/
        //【1】*******TransactionMap -> HBase
        Stream kafkaSream = topology.newStream("kfkSpout",
                new TransactionalTridentKafkaSpout(KfkTridentConfig.getKafkaConfig())).parallelismHint(10).shuffle()
                .each(new Fields(TableAndColumn.kafka_default_field), new KfkFunctions.SpoutSplitPrint(),
                        new Fields(TableAndColumn._column_shopId, TableAndColumn._column_shopAmt)).parallelismHint(8);


        TridentState kfkstate = kafkaSream.groupBy(new Fields(TableAndColumn._column_shopId))

                .persistentAggregate(KfkHBaseMapState.transactional(KfkStateOptions.getShopRankingOptions()),
                        new Fields(TableAndColumn._column_shopId, TableAndColumn._column_shopAmt),
                        new KfkAggregator.shopRanking_CombinerAggre_Sum(),
                        new Fields(TableAndColumn._column_shopAmtSum));


        //【2】*******Insert Mysql for TopN Data
        TridentState kfkstateMem = kfkstate.newValuesStream()
                .each(new Fields(TableAndColumn._column_shopId, TableAndColumn._column_shopAmtSum),
                        new KfkFunctions.JDBCSplitPrint(), new Fields(TableAndColumn.column_shopId, TableAndColumn.column_shopAmtSum)).parallelismHint(6)
                .partitionPersist(KfkStateOptions.getJdbcStateFactory(),
                        new Fields(TableAndColumn.column_shopId, TableAndColumn.column_shopAmtSum),
                        new KfkJdbcUpdater());


        //【3】*******from DRPC and get TopN data (select shop_id,shop_amtSum from shop_ranking order by shop_amtSum desc limit 0,10;)


//
//
//        /**>>>>>>汇总量实时计算**/
        TridentState allAmt_kfkstate = kafkaSream.each(new Fields(TableAndColumn._column_shopId, TableAndColumn._column_shopAmt),
                new KfkFunctions.ForCreateAllKey(), new Fields(TableAndColumn.column_shopId, TableAndColumn.column_shopAmt)).parallelismHint(5)
                .groupBy(new Fields(TableAndColumn.column_shopId))
                .persistentAggregate(KfkHBaseMapState.transactional(KfkStateOptions.getAllAmtOptions()),
                        new Fields(TableAndColumn.column_shopAmt),
                        new KfkAggregator.allAmt_CombinerAggre_Sum(),
                        new Fields(TableAndColumn.column_shopAmtSum));
//
//
//
//        /**>>>>>XY曲线量实时计算**/
        TridentState allAmt_xy_kfkstate = allAmt_kfkstate.newValuesStream()
                .each(new Fields(TableAndColumn.column_shopAmtSum), new KfkFunctions.ForCreateAllKey_intoTime(), new Fields(TableAndColumn.x_time))
                .parallelismHint(3)
                .partitionPersist(new kfkHBaseStateFactory(KfkStateOptions.getOptions_XY()),
                        new Fields(TableAndColumn.x_time, TableAndColumn.column_shopAmtSum),
                        new KfkHBaseUpdater(), new Fields("dd", "ww"));


        /***************DRPC *****************/
        topology.newDRPCStream("shop_ranking", drpc)
                .each(new Fields("args"), new Split(), new Fields("re_key"))
                .groupBy(new Fields("re_key"))
                .stateQuery(kfkstateMem,
                        new Fields("re_key"),
                        new KfkJdbcQuery(),
                        new Fields("_cAmt"));


        topology.newDRPCStream("all_Amt", drpc)
                .each(new Fields("args"), new Split(), new Fields("re_key"))
                .groupBy(new Fields("re_key"))
                .stateQuery(allAmt_kfkstate,
                        new Fields("re_key"),
                        new MapGet(),
                        new Fields("_cAmt"));

        topology.newDRPCStream("xtime_Amt", drpc)
                .each(new Fields("args"), new Split(), new Fields("re_key"))
                .groupBy(new Fields("re_key"))
                .stateQuery(allAmt_xy_kfkstate,
                        new Fields("re_key"),
                        new KfkHBaseQuery(),
                        new Fields("_cAmt"));


        return topology.build();
    }


    /**
     * 主方法
     * LocalSubmitter为在本地环境下运行的封装好的方法。里面已经实现了LocalCluster对象及对Config其他
     * 参数的设定
     *
     * @param args
     */
    public static void main(String[] args) {

        if (args != null && args.length > 0) {
            try {
                final Config config = new Config();
                config.setMaxSpoutPending(20);
                config.setDebug(true);
                config.put("hbase", new HashedMap());

                StormSubmitter.submitTopology(args[0], config, MainTopology.buildTopology(null));
                //TestTopology.kfkPrintResults(localSubmitter.getDrpc(), 60, 10000, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            final String tpName = "kfkTopology";
            final LocalSubmitter localSubmitter = LocalSubmitter.newInstance();

            Config config = LocalSubmitter.defaultConfig();
            config.put("hbase", new HashedMap());
            //提供并运行topology
            localSubmitter.submit(tpName, config, MainTopology.buildTopology(localSubmitter.getDrpc()));
            KfkLocalDrpc.kfkPrintResults(localSubmitter.getDrpc(), 60, 10000, TimeUnit.SECONDS);
        }
    }

}
