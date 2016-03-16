package ufrj.gta;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;


import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


public class KafkaSpoutConfig
{
    SpoutConfig kafkaConfig;

    public KafkaSpoutConfig(String zookeeperHost, String port, String topicName, String id){
      ZkHosts zkHosts=new ZkHosts(zookeeperHost + ":" + port);
      String zkRoot="";
      kafkaConfig=new SpoutConfig(zkHosts, topicName, zkRoot, id);
      kafkaConfig.scheme=new SchemeAsMultiScheme(new StringScheme());
      kafkaConfig.forceFromStart=true;
    }

    public SpoutConfig getConfig(){
      return kafkaConfig;
    }

}
