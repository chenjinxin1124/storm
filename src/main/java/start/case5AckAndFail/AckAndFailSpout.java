package start.case5AckAndFail;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 1. 读取 src/main/java/start/case5AckAndFail/order.log 文件
 * 2. 按行读取文件内容，缓存，发送
 */
public class AckAndFailSpout implements IRichSpout {

    FileInputStream fis;
    InputStreamReader isr;
    BufferedReader br;
    private ConcurrentHashMap<Object, Values> _pending;//线程安全的Map，存储emit过的tuple
    private ConcurrentHashMap<Object, Integer> fail_pending;//存储失败的tuple和其失败次数
    SpoutOutputCollector collector = null;

    String str = null;

    //初始化函数
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.fis = new FileInputStream("src/main/java/start/case5AckAndFail/order.log");
            this.isr = new InputStreamReader(fis, "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        this.br = new BufferedReader(isr);
        _pending = new ConcurrentHashMap<>();
        fail_pending = new ConcurrentHashMap<>();
    }

    @Override
    public void close() {
        try {
            br.close();
            isr.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 打印最终没有处理成功的tuple
        for (Map.Entry<Object, Integer> entry : fail_pending.entrySet()) {
            System.out.println("Key = " + entry.getKey().toString() + ", Value = " + entry.getValue());
        }
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    // 按行读取文件内容，缓存，发送
    @Override
    public void nextTuple() {
        try {
            while ((str = this.br.readLine()) != null) {
                UUID msgId = UUID.randomUUID();
                String[] split = str.split("\t");
                String orderAmt = split[1];
                String date = split[2].substring(0, 10);
                Values values = new Values(date, orderAmt);

                this._pending.put(msgId, values);

                collector.emit(values, msgId);

                System.out.println("_pending.size()=" + _pending.size());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void ack(Object msgId) {
        System.out.println("_pending size 共有:" + _pending.size());
        System.out.println("spout ack:" + msgId.toString() + "---" + msgId.getClass());
        this._pending.remove(msgId);
        System.out.println("_pending size 剩余:" + _pending.size());
        System.out.println("-------------------------------------------------------------------------------");
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("spout fail:" + msgId.toString());
        Integer fail_count = fail_pending.get(msgId);//获取该Tuple失败的次数
        if (fail_count == null) {
            fail_count = 0;
        }
        fail_count++;

        if (fail_count >= 3) {
            //重试次数已满，不再进行重新emit
            // fail_pending.remove(msgId);
        } else {
            //记录该tuple失败次数
            fail_pending.put(msgId, fail_count);
            //重发
            this.collector.emit(this._pending.get(msgId), msgId);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "orderAmt"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
