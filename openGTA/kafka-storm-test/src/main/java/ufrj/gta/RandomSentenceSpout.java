package ufrj.gta;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import java.io.*;

public class RandomSentenceSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  String sentence;


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void nextTuple() {
    Utils.sleep(3000);
    try{
      sentence = new Scanner(new File("/vagrant/kafka-storm-kmeans/src/main/java/ufrj/gta/teste.JSON")).useDelimiter("\\Z").next();
    }
    catch(Exception e){
      System.out.println(e);
    }
    _collector.emit(new Values(sentence));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sentence"));
  }

}
