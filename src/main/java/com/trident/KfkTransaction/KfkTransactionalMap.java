package com.trident.KfkTransaction;

import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.ValueUpdater;
import org.apache.storm.trident.state.map.CachedBatchReadsMap;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.MapState;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2018/5/26.
 */
public class KfkTransactionalMap<T> implements MapState<T> {
    public static <T> MapState<T> build(IBackingMap<TransactionalValue> backing) {
        return new KfkTransactionalMap<T>(backing);
    }

    CachedBatchReadsMap<TransactionalValue> _backing;
    Long _currTx;

    protected KfkTransactionalMap(IBackingMap<TransactionalValue> backing) {
        _backing = new CachedBatchReadsMap(backing);
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {

        List<CachedBatchReadsMap.RetVal<TransactionalValue>> vals = _backing.multiGet(keys);
        List<T> ret = new ArrayList<T>(vals.size());
        for(CachedBatchReadsMap.RetVal<TransactionalValue> retval: vals) {
            TransactionalValue v = retval.val;
            if(v!=null) {
                ret.add((T) v.getVal());
            } else {
                ret.add(null);
            }
        }
        return ret;
        // return null;
    }




    @Override
    public List<T> multiUpdate(List<List<Object>> keys, List<ValueUpdater> updaters) {
        List<CachedBatchReadsMap.RetVal<TransactionalValue>> curr = _backing.multiGet(keys);
        List<TransactionalValue> newVals = new ArrayList<TransactionalValue>(curr.size());
        List<List<Object>> newKeys = new ArrayList();
        List<T> ret = new ArrayList<T>();
        for(int i=0; i<curr.size(); i++) {
            CachedBatchReadsMap.RetVal<TransactionalValue> retval = curr.get(i);
            TransactionalValue<T> val = retval.val;
            ValueUpdater<T> updater = updaters.get(i);
            TransactionalValue<T> newVal;
            boolean changed = false;
            if(val==null) {
               // newVal = new TransactionalValue<T>(_currTx, updater.update(null));
                newVal = new TransactionalValue<T>(_currTx, updater.update(null));

                changed = true;
            } else {
                if(_currTx!=null && _currTx.equals(val.getTxid()) && !retval.cached) {
                    newVal = val;
                } else {
                    newVal = new TransactionalValue<T>(_currTx, updater.update(val.getVal()));
                    changed = true;
                }
            }


            ret.add(newVal.getVal());
            if(changed) {
                newVals.add(newVal);
                newKeys.add(keys.get(i));
            }
        }
        if(!newKeys.isEmpty()) {
            _backing.multiPut(newKeys, newVals);
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        List<TransactionalValue> newVals = new ArrayList<TransactionalValue>(vals.size());
        for(T val: vals) {
            newVals.add(new TransactionalValue<T>(_currTx, val));
        }

        _backing.multiPut(keys, newVals);
    }

    @Override
    public void beginCommit(Long txid) {
        _currTx = txid;
        _backing.reset();
       // System.out.println("beginCommit方法返：>>>>>>>>>>>>>>>>>>>>>>_backing.reset()");
    }

    @Override
    public void commit(Long txid) {
        _currTx = null;
        _backing.reset();
        //System.out.println("commit：>>>>>>>>>>>>>>>>>>>>>>_backing.reset()");
    }
}
