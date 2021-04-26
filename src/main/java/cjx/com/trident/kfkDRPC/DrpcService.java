package cjx.com.trident.kfkDRPC;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

/**
 * Created by Administrator on 2018/6/20.
 */
public class DrpcService {


    public static DRPCClient getDRPCClient() {

       // Map config = Utils.readDefaultConfig();
        DRPCClient drpcService = null;

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("drpc.authorizer.acl.strict", false);
        conf.put("storm.thrift.transport", "org.apache.storm.security.auth.SimpleTransportPlugin");
        conf.put("storm.nimbus.retry.times", 5);
        conf.put("storm.nimbus.retry.interval.millis", 2000);
        conf.put("storm.nimbus.retry.intervalceiling.millis", 60000);
        conf.put("drpc.max_buffer_size", 1048576);
        try {
            drpcService = new DRPCClient(conf, "bigdata-pro01", 3772);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return drpcService;
    }
}
