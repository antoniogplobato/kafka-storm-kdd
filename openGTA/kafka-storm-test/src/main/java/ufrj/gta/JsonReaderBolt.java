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

import java.util.Map;

import org.json.simple.*;
import org.json.simple.parser.JSONParser;

public class JsonReaderBolt extends BaseRichBolt
{
  private OutputCollector collector;
  private String [] keys;
  private int qtdKeys;

  public JsonReaderBolt(String [] receivedKeys){
    keys = receivedKeys;
    qtdKeys = keys.length;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple)
  {
    JSONParser parser = new JSONParser();
    try{
      Object obj = parser.parse(tuple.getString(0));
      JSONObject jsonObj = (JSONObject) obj;
      Values values = new Values();
      for(String key : keys){
        String temp = (String) jsonObj.get(key);
        values.add(temp);
      }
      collector.emit(values);
    }
    catch(Exception e){
      System.out.println(e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields(keys));
  }
}
