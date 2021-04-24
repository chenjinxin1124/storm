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
package com.trident.kfkTridentHBase;

import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapper;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.storm.hbase.common.Utils.toBytes;
import static org.apache.storm.hbase.common.Utils.toLong;

/**
 *
 */
public class KfkSimpleTridentHBaseMapper implements TridentHBaseMapper {
    private static final Logger LOG = LoggerFactory.getLogger(KfkSimpleTridentHBaseMapper.class);

    private String rowKeyField;
    private byte[] columnFamily;
    private Fields columnFields;
    private Fields counterFields;

    public KfkSimpleTridentHBaseMapper(){
    }


    public KfkSimpleTridentHBaseMapper withRowKeyField(String rowKeyField){
        this.rowKeyField = rowKeyField;
        return this;
    }

    public KfkSimpleTridentHBaseMapper withColumnFields(Fields columnFields){
        this.columnFields = columnFields;
        return this;
    }

    public KfkSimpleTridentHBaseMapper withCounterFields(Fields counterFields){
        this.counterFields = counterFields;
        return this;
    }

    public KfkSimpleTridentHBaseMapper withColumnFamily(String columnFamily){
        this.columnFamily = columnFamily.getBytes();
        return this;
    }


    @Override
    public byte[] rowKey(TridentTuple tuple) {
        Object objVal = tuple.getValueByField(this.rowKeyField);
        return toBytes(objVal);
    }

    @Override
    public ColumnList columns(TridentTuple tuple) {
        ColumnList cols = new ColumnList();
        if(this.columnFields != null){
            // TODO timestamps
            for(String field : this.columnFields){
                String value = String.valueOf(tuple.getValueByField(field));
                cols.addColumn(this.columnFamily, field.getBytes(), toBytes(value));
            }
        }
        if(this.counterFields != null){
            for(String field : this.counterFields){
                cols.addCounter(this.columnFamily, field.getBytes(), toLong(tuple.getValueByField(field)));
            }
        }
        return cols;
    }
}
