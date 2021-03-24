package start.case6TridentTest;

import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class DrpcClientTest {
    public static void main(String[] args) {
        try {
            Map<String, Object> conf = Utils.readDefaultConfig();
            DRPCClient client = new DRPCClient(conf, "bigdata-pro01", 3772);
            while (true) {
                String result = client.execute("words", "2020-01-11 2020-01-12 2020-01-13 2020-01-14");
                System.out.println(result);
                Thread.sleep(1000);
            }
        } catch (TException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
