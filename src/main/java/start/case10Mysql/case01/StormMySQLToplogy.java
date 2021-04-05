package start.case10Mysql.case01;

import com.google.common.collect.Lists;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.jdbc.trident.state.JdbcStateFactory;
import org.apache.storm.jdbc.trident.state.JdbcUpdater;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import start.case9HbaseKafka.case02.TridentKafkaConsumerTopology;
import start.case9HbaseKafka.case02.TridentKafkaWordCount;

import java.util.List;
import java.util.Map;

public class StormMySQLToplogy {

    public static void main(String[] args) {
        Config conf = new Config();
        conf.setMaxSpoutPending(5);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCounter", conf, getTopology());

    }

    public static StormTopology getTopology() {
        TridentTopology topology = new TridentTopology();

        // 数据库连接信息
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://bigdata-pro01:3306/storm");
        hikariConfigMap.put("dataSource.user", "root");
        hikariConfigMap.put("dataSource.password", "123456");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        // 创建数据表信息
        List<Column> columnSchema = Lists.newArrayList(
                new Column("order_date", java.sql.Types.VARCHAR),
                new Column("order_amt", java.sql.Types.VARCHAR));
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columnSchema);

        JdbcState.Options options = new JdbcState.Options()
                .withConnectionProvider(connectionProvider)
                .withMapper(simpleJdbcMapper)
                .withTableName("stormMysql_test");

        JdbcStateFactory jdbcStateFactory = new JdbcStateFactory(options);
        String zkUrl = "bigdata-pro01:2181,bigdata-pro02:2181,bigdata-pro03:2181";
        TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(TridentKafkaWordCount.newTridentKafkaConfig(zkUrl));
        topology.newStream("userSpout", kafkaSpout)
                .each(new Fields("str"), new TridentKafkaConsumerTopology.MyFunction(), new Fields("order_date", "order_amt"))
                .partitionPersist(jdbcStateFactory, new Fields("order_date", "order_amt"), new JdbcUpdater(), new Fields());
        return topology.build();
    }

}
