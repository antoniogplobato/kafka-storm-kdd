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

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class KmeansTopology
{
    public static void main( String[] args )
    {
      KafkaSpoutConfig kafkaSpoutConfig = new KafkaSpoutConfig("node2", "2181", "test", "01");
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig.getConfig()), 1);
      // builder.setSpout("random-sentence-spout", new RandomSentenceSpout(), 1);
      // String[] tempArray = {"ip", "name", "messages", "size"};
      String [] tempArray = {"timestamp","srcIP","dstIP","L7protocol","size","ttl","srcMAC","dstMAC","L4protocol","srcPort","dstPort","payload"};
      builder.setBolt("json-reader-bolt", new JsonReaderBolt(tempArray), 1).globalGrouping("kafka-spout");
      // builder.setBolt("json-reader-bolt", new JsonReaderBolt(tempArray), 1).globalGrouping("random-sentence-spout");
      builder.setBolt("printer-bolt", new PrinterBolt(tempArray),1).globalGrouping("json-reader-bolt");
      Config config=new Config();
      LocalCluster cluster=new LocalCluster();
      cluster.submitTopology("KafkaConsumerTopology", config, builder.createTopology());
      try{
        Thread.sleep(60000);
      }
      catch(InterruptedException ex){
        ex.printStackTrace();
      }
      cluster.killTopology("KafkaConsumerTopology");
      cluster.shutdown();
    }
}
