package com.trident.kfkTridentUtil;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.trident.kfkConfig.MySqlConfig;
import com.trident.kfkConfig.TableAndColumn;
import com.trident.kfkTridentHBase.KfkHBaseMapState;
import com.trident.kfkTridentHBase.KfkHBaseState;
import com.trident.kfkTridentHBase.KfkHBaseValueMapper;
import com.trident.kfkTridentHBase.KfkSimpleTridentHBaseMapper;
import com.trident.kfkTridentJDBC.KfkJdbcState;
import com.trident.kfkTridentJDBC.KfkJdbcStateFactory;
import com.trident.kfkTridentJDBC.KfkSimpleJdbcUpdateMapper;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapMapper;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseMapState;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/6/21.
 */
public class KfkStateOptions {


    /**
     * state数据保存逻辑******************************【jdbcState】*************************
     */
    public static KfkJdbcStateFactory getJdbcStateFactory(){
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName", MySqlConfig.dataSourceClassName);
        hikariConfigMap.put("dataSource.url", MySqlConfig.dataSource_url);
        hikariConfigMap.put("dataSource.user",MySqlConfig.dataSource_user);
        hikariConfigMap.put("dataSource.password",MySqlConfig.dataSource_password);
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        List<Column> columnSchema = Lists.newArrayList(
                new Column(TableAndColumn.column_shopId, Types.VARCHAR),
                new Column(TableAndColumn.column_shopAmtSum, Types.VARCHAR));

        String tableName = TableAndColumn.MYSQL_SHOP_RANKING;
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columnSchema);

        KfkJdbcState.Options options = new KfkJdbcState.Options()
                .withConnectionProvider(connectionProvider)
                .withMapper(simpleJdbcMapper)
                .withJdbcLookupMapper(new SimpleJdbcLookupMapper(new Fields(TableAndColumn.column_shopId,TableAndColumn.column_shopAmtSum),
                        Lists.newArrayList(new Column(TableAndColumn.column_shopId, Types.VARCHAR))))
                .withTableName(tableName)
                .withSelectQuery("select * from shop_ranking where shop_id=?")
                .withSelectAllQuery("select shop_id,shop_amtSum from shop_ranking order by shop_amtSum desc limit 0,10;")
                .withQueryTimeoutSecs(30)
                .withUpdateQuery("update shop_ranking set shop_amtSum =? where shop_id=?")
                .withJdbcUpdateMapper(new KfkSimpleJdbcUpdateMapper(Lists.newArrayList(
                        new Column(TableAndColumn.column_shopAmtSum, Types.VARCHAR),
                        new Column(TableAndColumn.column_shopId, Types.VARCHAR)
                )));


        KfkJdbcStateFactory jdbcStateFactory = new KfkJdbcStateFactory(options);
        return jdbcStateFactory;

    }


    /**
     * state数据保存逻辑******************************【hbaseState】*************************
     */
    public static KfkHBaseMapState.Options getShopRankingOptions(){

        KfkHBaseMapState.Options option = new KfkHBaseMapState.Options();
        option.tableName = TableAndColumn.HBASE_TRUN_SHOP_RANKING;
        option.columnFamily = TableAndColumn.HBASE_FAMILY;
        option.mapMapper = new SimpleTridentHBaseMapMapper(TableAndColumn.column_shopAmtSum);

        return option;
    }

    public static HBaseMapState.Options getShopRankingOptions_q(){

        HBaseMapState.Options option = new HBaseMapState.Options();
        option.tableName = TableAndColumn.HBASE_TRUN_SHOP_RANKING;
        option.columnFamily = TableAndColumn.HBASE_FAMILY;
        option.mapMapper = new SimpleTridentHBaseMapMapper(TableAndColumn.column_shopAmtSum);

        return option;
    }


    public static KfkHBaseMapState.Options getAllAmtOptions(){

        KfkHBaseMapState.Options option = new KfkHBaseMapState.Options();
        option.tableName = TableAndColumn.HBASE_TRUN_ALL_AMT_SUM;
        option.columnFamily = TableAndColumn.HBASE_FAMILY;

        option.mapMapper = new SimpleTridentHBaseMapMapper(TableAndColumn.column_allAmtSum);


        return option;
    }

    public static KfkHBaseState.Options getOptions_XY(){
        TridentHBaseMapper tridentHBaseMapper = new KfkSimpleTridentHBaseMapper()
                .withColumnFamily(TableAndColumn.HBASE_FAMILY)
                .withColumnFields(new Fields(TableAndColumn.x_time,TableAndColumn.column_shopAmtSum))
                .withRowKeyField(TableAndColumn.x_time);

        HBaseValueMapper rowToStormValueMapper = new KfkHBaseValueMapper();

        HBaseProjectionCriteria projectionCriteria = new HBaseProjectionCriteria();
        projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData(TableAndColumn.HBASE_FAMILY, TableAndColumn.x_time));
        projectionCriteria.addColumn(new HBaseProjectionCriteria.ColumnMetaData(TableAndColumn.HBASE_FAMILY, TableAndColumn.column_shopAmtSum));


        KfkHBaseState.Options options = new KfkHBaseState.Options()
                .withConfigKey("hbase")
                .withDurability(Durability.SYNC_WAL)
                .withMapper(tridentHBaseMapper)
                .withProjectionCriteria(projectionCriteria)
                .withRowToStormValueMapper(rowToStormValueMapper)
                .withTableName(TableAndColumn.HBASE_XTIME_AMT_SUM);
        return options;
    }

}
