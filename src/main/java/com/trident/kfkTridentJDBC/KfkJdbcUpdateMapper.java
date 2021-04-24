package com.trident.kfkTridentJDBC;

import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Created by Administrator on 2018/6/18.
 */
public interface KfkJdbcUpdateMapper extends JdbcMapper {
    List<Values> toTuple(ITuple var1, List<Column> var2);

    void declareOutputFields(OutputFieldsDeclarer var1);
}
