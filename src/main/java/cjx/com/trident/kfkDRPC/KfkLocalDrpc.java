package cjx.com.trident.kfkDRPC;

import cjx.com.trident.MainTopology;
import org.apache.storm.LocalDRPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2018/6/21.
 */
public class KfkLocalDrpc {

    private static final Logger LOG = LoggerFactory.getLogger(MainTopology.class);

    public static void kfkPrintResults(LocalDRPC drpc, int _num, int sleepTime, TimeUnit sleepUnit) {
        for (int i = 0; i < _num; i++) {
            try {
                LOG.info("-------【DRPC_shop_ranking】 " + drpc.execute("shop_ranking", "23"));
                LOG.info("-------【DRPC_all_amt】 " + drpc.execute("all_Amt", "kfk-all"));
                LOG.info("-------【DRPC_xtime_amt】 " + drpc.execute("xtime_Amt", "20180622-23:45:13"));

                System.out.println();
                Thread.sleep(sleepUnit.toMillis(sleepTime));
            } catch (Exception e) {
                e.printStackTrace();
                //throw new RuntimeException(e);
            }
        }
    }
}
