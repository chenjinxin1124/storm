package cjx.com.trident.kfkTridentHBase;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.mapper.HBaseValueMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by Administrator on 2018/6/23.
 */
public class KfkHBaseValueMapper implements HBaseValueMapper {

    @Override
    public List<Values> toValues(ITuple tuple, Result result) throws Exception {
        List<Values> values = new ArrayList<Values>();
        Cell[] cells = result.rawCells();
        for(Cell cell : cells) {

            Values value = new Values (Bytes. toString (CellUtil. cloneQualifier (cell)), new String(Bytes. toString (CellUtil. cloneValue (cell))));
            values.add(value);

        }
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userid","cAmt"));
    }

}
