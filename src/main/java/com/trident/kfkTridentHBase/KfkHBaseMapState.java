package com.trident.kfkTridentHBase;

import com.google.common.collect.Maps;
import com.trident.KfkTransaction.KfkTransactionalMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.hbase.common.Utils;
import org.apache.storm.hbase.security.HBaseSecurityUtil;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapMapper;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.apache.storm.hbase.trident.state.HBaseMapState;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.state.*;
import org.apache.storm.trident.state.map.*;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/5/26.
 */
public class KfkHBaseMapState<T> implements IBackingMap<T> {
    private static Logger LOG = LoggerFactory.getLogger(HBaseMapState.class);

    private int partitionNum;


    @SuppressWarnings("rawtypes")
    private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = Maps.newHashMap();

    static {
        DEFAULT_SERIALZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    private Options<T> options;
    private Serializer<T> serializer;
    private HTable table;

    public KfkHBaseMapState(final Options<T> options, Map map, int partitionNum) {
        this.options = options;
        this.serializer = options.serializer;
        this.partitionNum = partitionNum;

        final Configuration hbConfig = HBaseConfiguration.create();
        Map<String, Object> conf = (Map<String, Object>)map.get(options.configKey);
        if(conf == null){
            LOG.info("HBase configuration not found using key '" + options.configKey + "'");
            LOG.info("Using HBase config from first hbase-site.xml found on classpath.");
        } else {
            if (conf.get("hbase.rootdir") == null) {
                LOG.warn("No 'hbase.rootdir' value found in configuration! Using HBase defaults.");
            }
            for (String key : conf.keySet()) {
                hbConfig.set(key, String.valueOf(conf.get(key)));
            }
        }



            LOG.error("map '" + map + "'");
            LOG.error("hbConfig '" + hbConfig + "'");
            LOG.error("tableName '" + options.tableName + "'");
        UserProvider provider = null;
       try {

            provider = HBaseSecurityUtil.login(map, hbConfig);
           LOG.error("provider '" +provider + "'");
       }catch (Exception e ){
           LOG.error(e.getMessage());
           LOG.error("provider '" +provider + "'");
       }
        try{
            this.table = Utils.getTable(provider, hbConfig, options.tableName);
            LOG.error("this.table '" +this.table + "'");
        } catch(Exception e){
            LOG.error("provider '" +this.table + "'");
            throw new RuntimeException("HBase bolt preparation failed: " + e.getMessage(), e);
        }

    }

    public static class KfkOptions implements Serializable {
        private TridentHBaseMapper mapper;
        private Durability durability = Durability.SKIP_WAL;
        private HBaseProjectionCriteria projectionCriteria;
        private HBaseValueMapper rowToStormValueMapper;
        private String configKey;
        private String tableName;

        public KfkOptions withDurability(Durability durability) {
            this.durability = durability;
            return this;
        }

        public KfkOptions withProjectionCriteria(HBaseProjectionCriteria projectionCriteria) {
            this.projectionCriteria = projectionCriteria;
            return this;
        }

        public KfkOptions withConfigKey(String configKey) {
            this.configKey = configKey;
            return this;
        }

        public KfkOptions withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public KfkOptions withRowToStormValueMapper(HBaseValueMapper rowToStormValueMapper) {
            this.rowToStormValueMapper = rowToStormValueMapper;
            return this;
        }

        public KfkOptions withMapper(TridentHBaseMapper mapper) {
            this.mapper = mapper;
            return this;
        }
    }


    public static class Options<T> implements Serializable {

        public Serializer<T> serializer = null;
        public int cacheSize = 5000;
        public String globalKey = "$HBASE_STATE_GLOBAL$";
        public String configKey = "hbase.config";
        public String tableName;
        public String columnFamily;
        public TridentHBaseMapMapper mapMapper;
        private TridentHBaseMapper mapper;
    }


    @SuppressWarnings("rawtypes")
    public static StateFactory opaque() {
        Options<OpaqueValue> options = new Options<OpaqueValue>();
        return opaque(options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory opaque(Options<OpaqueValue> opts) {

        return new Factory(StateType.OPAQUE, opts);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional() {
        Options<TransactionalValue> options = new Options<TransactionalValue>();
        return transactional(options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional(Options<TransactionalValue> opts) {
        return new Factory(StateType.TRANSACTIONAL, opts);
    }

    public static StateFactory nonTransactional() {
        Options<Object> options = new Options<Object>();
        return nonTransactional(options);
    }

    public static StateFactory nonTransactional(Options<Object> opts) {
        return new Factory(StateType.NON_TRANSACTIONAL, opts);
    }


    protected static class Factory implements StateFactory {
        private StateType stateType;
        private Options options;

        @SuppressWarnings({"rawtypes", "unchecked"})
        public Factory(StateType stateType, Options options) {
            this.stateType = stateType;
            this.options = options;

            if (this.options.serializer == null) {
                this.options.serializer = DEFAULT_SERIALZERS.get(stateType);
            }

            if (this.options.serializer == null) {
                throw new RuntimeException("Serializer should be specified for type: " + stateType);
            }

            if (this.options.mapMapper == null) {
                throw new RuntimeException("MapMapper should be specified for type: " + stateType);
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
            LOG.info("Preparing HBase State for partition {} of {}.", partitionIndex + 1, numPartitions);
            IBackingMap state = new KfkHBaseMapState(options, conf, partitionIndex);

            if(options.cacheSize > 0) {
                state = new CachedMap(state, options.cacheSize);
            }

            MapState mapState = null;
            switch (stateType) {
                case NON_TRANSACTIONAL:
                   // mapState = NonTransactionalMap.build(state);
                    break;

                case OPAQUE:
                    mapState = OpaqueMap.build(state);
                    break;

                case TRANSACTIONAL:
                    mapState = KfkTransactionalMap.build(state);
                    //System.out.println("***************************************mapState"+mapState);
                    break;

                default:
                    throw new IllegalArgumentException("Unknown state type: " + stateType);
            }
            return new SnapshottableMap(mapState,new Values(options.globalKey));
        }

    }


    public List<T> multiGet(List<List<Object>> keys) {

        //System.out.println(">>>>>>>>>>>>>>>>【multiGetS  key 】>>>>>>>>>>>>>>>>>"+keys);
        List<Get> gets = new ArrayList<Get>();
        for(List<Object> key : keys){
            byte[] hbaseKey = this.options.mapMapper.rowKey(key);
            String qualifier = this.options.mapMapper.qualifier(key);


            LOG.info("Partition: {}, GeT: {}", this.partitionNum, new String(hbaseKey));
            Get get = new Get(hbaseKey);
            get.addColumn(this.options.columnFamily.getBytes(), qualifier.getBytes());
            gets.add(get);
        }



        List<T> retval = new ArrayList<T>();
        try {
            Result[] results = this.table.get(gets);
            for (int i = 0; i < keys.size(); i++) {
                String qualifier = this.options.mapMapper.qualifier(keys.get(i));
                Result result = results[i];
                byte[] value = result.getValue(this.options.columnFamily.getBytes(), qualifier.getBytes());

                if(value != null) {
                    retval.add(this.serializer.deserialize(value));
                } else {
                    retval.add(null);
                }
            }
        } catch(IOException e){
            throw new FailedException("IOException while reading from HBase.", e);
        }
        //System.out.println("KfkHBaseMapState =>> mutilGet方法返回值：>>>"+retval);
        return retval;
    }



    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        System.out.println("KfkHBaseMapState =>> multiPut values 方法返回值：>>>"+values);

        List<Put> puts = new ArrayList<Put>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            byte[] hbaseKey = this.options.mapMapper.rowKey(keys.get(i));

            String qualifier = this.options.mapMapper.qualifier(keys.get(i));

            LOG.info("Partiton: {}, Key: {}, Value: {}", new Object[]{this.partitionNum, new String(hbaseKey), new String(this.serializer.serialize(values.get(i)))});
            Put put = new Put(hbaseKey);
            T val = values.get(i);
            put.add(this.options.columnFamily.getBytes(),
                    qualifier.getBytes(),
                    this.serializer.serialize(val));
            puts.add(put);
        }
        try {
            this.table.put(puts);
        } catch (InterruptedIOException e) {
            throw new FailedException("Interrupted while writing to HBase", e);
        } catch (RetriesExhaustedWithDetailsException e) {
            throw new FailedException("Retries exhaused while writing to HBase", e);
        } catch (IOException e) {
            throw new FailedException("IOException while writing to HBase", e);
        }


    }
}
