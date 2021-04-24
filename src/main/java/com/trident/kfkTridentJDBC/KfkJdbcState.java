//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.trident.kfkTridentJDBC;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class KfkJdbcState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(KfkJdbcState.class);
    private Options options;
    private KfkJdbcClient kfkJdbcClient;
    private Map map;

    protected KfkJdbcState(Map map, int partitionIndex, int numPartitions, Options options) {
        this.options = options;
        this.map = map;
    }

    protected void prepare() {
        this.options.connectionProvider.prepare();
        if(StringUtils.isBlank(this.options.insertQuery) && StringUtils.isBlank(this.options.tableName) && StringUtils.isBlank(this.options.selectQuery)) {
            throw new IllegalArgumentException("If you are trying to insert into DB you must supply either insertQuery or tableName.If you are attempting to user a query state you must supply a select query.");
        } else {
            if(this.options.queryTimeoutSecs == null) {
                this.options.queryTimeoutSecs = Integer.valueOf(Integer.parseInt(this.map.get("topology.message.timeout.secs").toString()));
            }

            this.kfkJdbcClient = new KfkJdbcClient(this.options.connectionProvider, this.options.queryTimeoutSecs.intValue());
        }
    }

    public void beginCommit(Long aLong) {
        LOG.debug("beginCommit is noop.");
    }

    public void commit(Long aLong) {
        LOG.debug("commit is noop.");
    }

    /**
     * 汇总数据 更新  （如果有就update，如果没有就insert）
     * @param tuples
     * @param collector
     */
    public void updateState(List<TridentTuple> tuples, TridentCollector collector) {

        try {
            List<List<Column>> columnsLists_update = new ArrayList();
            List<List<Column>> columnsLists_insert = new ArrayList();
            if(!StringUtils.isBlank(this.options.tableName)) {
                Iterator var3 = tuples.iterator();
                while(var3.hasNext()) {
                    TridentTuple tuple = (TridentTuple) var3.next();
                    List<Column> columns = this.options.jdbcLookupMapper.getColumns(tuple);
                    List<List<Column>> rows = this.kfkJdbcClient.select(this.options.selectQuery, columns);
                    if (rows.size() > 0) {
                        columnsLists_update.add(this.options.jdbcUpdateMapper.getColumns(tuple));
                    }else{
                        columnsLists_insert.add(this.options.mapper.getColumns(tuple));
                    }

                }

                    if (columnsLists_insert.size() > 0) {
                        this.kfkJdbcClient.insert(this.options.tableName, columnsLists_insert);
                    }
                    if (columnsLists_update.size() > 0) {
                        this.kfkJdbcClient.executeSelectAndInsertOrUpdate(this.options.updateSql, columnsLists_update);
                    }

            }

        } catch (Exception var6) {
            LOG.warn("Batch write failed but some requests might have succeeded. Triggering replay.", var6);
            throw new FailedException(var6);
        }
    }

    /**
    public List<List<Values>> batchRetrieve(List<TridentTuple> tridentTuples) {
        ArrayList batchRetrieveResult = Lists.newArrayList();

        try {
            Iterator var3 = tridentTuples.iterator();

            while(var3.hasNext()) {
                TridentTuple tuple = (TridentTuple)var3.next();
                List<Column> columns = this.options.jdbcLookupMapper.getColumns(tuple);
                List<List<Column>> rows = this.kfkJdbcClient.select(this.options.selectQuery , columns);
                Iterator var7 = rows.iterator();

                while(var7.hasNext()) {
                    List<Column> row = (List)var7.next();
                    List<Values> values = this.options.jdbcLookupMapper.toTuple(tuple, row);
                    batchRetrieveResult.add(values);
                }
            }

            return batchRetrieveResult;
        } catch (Exception var10) {
            LOG.warn("Batch get operation failed. Triggering replay.", var10);
            throw new FailedException(var10);
        }
    }
    **/


    /**
     * 查排序完成之后的数据
     * @param tridentTuples
     * @return
     */
    public List<List<Values>> batchRetrieveAll(List<TridentTuple> tridentTuples) {
        ArrayList batchRetrieveResult = Lists.newArrayList();
        List retList = Lists.newArrayList();
        try {
            Iterator var3 = tridentTuples.iterator();

            while(var3.hasNext()) {
                TridentTuple tuple = (TridentTuple)var3.next();
                List<List<Column>> rows = this.kfkJdbcClient.selectAll(this.options.selectAllQuery);
                Iterator var7 = rows.iterator();

                while(var7.hasNext()) {
                    List<Column> row = (List)var7.next();
                    Map<String,Object> rowMap = new HashMap<String,Object>();
                    for(Column column : row){
                        rowMap.put(column.getColumnName(),column.getVal());
                    }
                    batchRetrieveResult.add(rowMap);
//                    List<Values> values = this.options.jdbcLookupMapper.toTuple(tuple, row);
//                    batchRetrieveResult.add(values);
                }
            }
            retList.add(batchRetrieveResult);
            return retList;
        } catch (Exception var10) {
            LOG.warn("Batch get operation failed. Triggering replay.", var10);
            throw new FailedException(var10);
        }
    }


    public static class Options implements Serializable {
        private JdbcMapper mapper;
        private KfkJdbcUpdateMapper jdbcUpdateMapper;
        private JdbcLookupMapper jdbcLookupMapper;
        private ConnectionProvider connectionProvider;
        private String tableName;
        private String insertQuery;
        private String selectQuery;
        private String selectAllQuery;
        private String updateSql;
        private Integer queryTimeoutSecs;


        public Options() {
        }

        public Options withConnectionProvider(ConnectionProvider connectionProvider) {
            this.connectionProvider = connectionProvider;
            return this;
        }

        public Options withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Options withInsertQuery(String insertQuery) {
            this.insertQuery = insertQuery;
            return this;
        }

        public Options withMapper(JdbcMapper mapper) {
            this.mapper = mapper;
            return this;
        }

        public Options withJdbcUpdateMapper(KfkJdbcUpdateMapper jdbcUpdateMapper) {
            this.jdbcUpdateMapper = jdbcUpdateMapper;
            return this;
        }

        public Options withJdbcLookupMapper(JdbcLookupMapper jdbcLookupMapper) {
            this.jdbcLookupMapper = jdbcLookupMapper;
            return this;
        }

        public Options withSelectQuery(String selectQuery) {
            this.selectQuery = selectQuery;
            return this;
        }
        public Options withSelectAllQuery(String selectAllQuery) {
            this.selectAllQuery = selectAllQuery;
            return this;
        }


        public Options withUpdateQuery(String updatgeQuery) {
            this.updateSql = updatgeQuery;
            return this;
        }


        public Options withQueryTimeoutSecs(int queryTimeoutSecs) {
            this.queryTimeoutSecs = Integer.valueOf(queryTimeoutSecs);
            return this;
        }
    }
}
