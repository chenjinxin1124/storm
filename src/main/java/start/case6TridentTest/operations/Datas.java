package start.case6TridentTest.operations;

import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Random;

public class Datas {
    Fields fields = new Fields("date", "amt", "city", "product");
    private final Integer[] amt = {1, 2, 4, 8, 16, 32, 64};
    private final String[] date = {"2020-01-11 12:23:34", "2020-01-12 12:23:34", "2020-01-13 12:23:34", "2020-01-14 12:23:34"};
    private final String[] city = {"beijing", "shanghai", "guangzhou", "shenzhen"};
    private final String[] product = {"128", "256", "512", "1024"};
    private final String[] bigData = {"Java", "Scala", "Hadoop", "Zookeeper", "Yarn", "Hive", "HBase", "Kafka",
            "Spark", "Spark Streaming", "Storm", "Flink"};
    private final Random random = new Random();

    public Integer getRandomAmt() {
        return amt[random.nextInt(amt.length)];
    }

    public String getRandomDate() {
        return date[random.nextInt(date.length)];
    }

    public String getRandomCity() {
        return city[random.nextInt(city.length)];
    }

    public String getRandomProduct() {
        return product[random.nextInt(product.length)];
    }

    public String getRandomBigData() {
        return bigData[random.nextInt(bigData.length)];
    }

    private final FixedBatchSpout spout = new FixedBatchSpout(fields,
            8,// 一批数据的最大数量
            new Values(getRandomDate(), getRandomAmt(), getRandomCity(), getRandomProduct()),
            new Values(getRandomDate(), getRandomAmt(), getRandomCity(), getRandomProduct()),
            new Values(getRandomDate(), getRandomAmt(), getRandomCity(), getRandomProduct()),
            new Values(getRandomDate(), getRandomAmt(), getRandomCity(), getRandomProduct()),
            new Values(getRandomDate(), getRandomAmt(), getRandomCity(), getRandomProduct()),
            new Values(getRandomDate(), getRandomAmt(), getRandomCity(), getRandomProduct()),
            new Values(getRandomDate(), getRandomAmt(), getRandomCity(), getRandomProduct()),
            new Values(getRandomDate(), getRandomAmt(), getRandomCity(), getRandomProduct()),
            new Values(getRandomDate(), getRandomAmt(), getRandomCity(), getRandomProduct()),
            new Values(getRandomDate(), getRandomAmt(), getRandomCity(), getRandomProduct()),
            new Values(getRandomDate(), getRandomAmt(), getRandomCity(), getRandomProduct())
    );

    public FixedBatchSpout getSpout() {
        return spout;
    }
}
