// cd /vagrant/kafka-storm-kdd/
// /opt/storm/bin/storm jar target/kafka-storm-kdd-1.0-SNAPSHOT-jar-with-dependencies.jar ufrj.gta.KddTopology
// /opt/storm/bin/storm kill KafkaConsumerTopology -w 5
// /opt/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test1
// /opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test1
// /opt/kafka/bin/kafka-topics.sh --describe --zookeeper localhost:2181

package ufrj.gta;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.metric.LoggingMetricsConsumer;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.io.PrintWriter;

public class KddTopology
{
    public static void main( String[] args )
    {
      int paralelismo = 5;
      TopologyBuilder builder = new TopologyBuilder();
      int numeroSpouts = paralelismo;
      String [] nomeSpouts = new String[numeroSpouts];
      for(int i = 0; i < numeroSpouts; i++){
        String nomeTopico = "test";
        String id = String.valueOf(i + 1);
        nomeTopico = nomeTopico + id;
        KafkaSpoutConfig kafkaSpoutConfig = new KafkaSpoutConfig("node2", "2181", nomeTopico, id);
        nomeSpouts[i] = "kafka-spout-" + id;
        builder.setSpout(nomeSpouts[i], new KafkaSpout(kafkaSpoutConfig.getConfig()), 1);
      }
      String [] tempArray = {"duration","protocol_type","service","flag","src_bytes","dst_bytes","land","wrong_fragment",
                             "urgent","hot","num_failed_logins","logged_in","num_compromised","root_shell","su_attempted",
                             "num_root","num_file_creations","num_shells","num_access_files","num_outbound_cmds","is_host_login",
                             "is_guest_login","count","srv_count","serror_rate","srv_serror_rate","rerror_rate","srv_rerror_rate",
                             "same_srv_rate","diff_srv_rate","srv_diff_host_rate","dst_host_count","dst_host_srv_count",
                             "dst_host_same_srv_rate","dst_host_diff_srv_rate","dst_host_same_src_port_rate",
                             "dst_host_srv_diff_host_rate","dst_host_serror_rate","dst_host_srv_serror_rate","dst_host_rerror_rate",
                             "dst_host_srv_rerror_rate", "type_of_attack"};
      // int contadorBolt = 0;
      // for(String nome : nomeSpouts){
      //   contadorBolt++;
      //   String nomeBolt = "all-bolt" + String.valueOf(contadorBolt);
      //   builder.setBolt(nomeBolt, new AllBolt(), 1).shuffleGrouping(nome);
      // }
      BoltDeclarer allBolt = builder.setBolt("all-bolt", new AllBolt(), paralelismo);
      for(String nome : nomeSpouts){
        allBolt.shuffleGrouping(nome);
      }
      Config config=new Config();
      config.setNumWorkers(12);
      // config.setMessageTimeoutSecs(11*60);
      config.setNumAckers(0);
      // config.setMaxSpoutPending(5000);
      // config.setDebug(true);
      config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1); // This will simply log all Metrics received into $STORM_HOME/logs/metrics.log on one or more worker nodes.
      try{
        StormSubmitter.submitTopology("KafkaConsumerTopology", config, builder.createTopology());
      }
      catch(Exception ex2){
        System.out.println(ex2);
      }
      // LocalCluster cluster=new LocalCluster();
      // cluster.submitTopology("KafkaConsumerTopology", config, builder.createTopology());
      // try{
      //   Thread.sleep(2*60000);
      // }
      // catch(InterruptedException ex){
      //   ex.printStackTrace();
      // }
      // cluster.killTopology("KafkaConsumerTopology");
      // cluster.shutdown();
      // BoltDeclarer csvBolt = builder.setBolt("csv-reader-bolt", new CsvReaderBolt(tempArray), 6);
      // for(String nome : nomeSpouts){
      //   csvBolt.shuffleGrouping(nome);
      // }
      // builder.setBolt("predict-bolt", new PredictBolt(),6).shuffleGrouping("csv-reader-bolt");
      // builder.setBolt("result-bolt", new ResultBolt(),6).shuffleGrouping("predict-bolt");

    }
}
