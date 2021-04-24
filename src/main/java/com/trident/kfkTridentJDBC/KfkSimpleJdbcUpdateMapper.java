//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.trident.kfkTridentJDBC;

import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.List;

public class KfkSimpleJdbcUpdateMapper extends SimpleJdbcMapper implements KfkJdbcUpdateMapper {


    public KfkSimpleJdbcUpdateMapper(List<Column> queryColumns) {
        super(queryColumns);

    }

    public List<Values> toTuple(ITuple input, List<Column> columns) {

            return null;

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
