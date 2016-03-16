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
import java.util.ArrayList;
import java.util.List;

public class CsvReaderBolt extends BaseRichBolt
{
  private OutputCollector collector;
  private String [] keys;

  public CsvReaderBolt(String [] receivedKeys){
    keys = receivedKeys;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple)
  {
    String[] dadosStr = tuple.getString(0).split(" ");
    List<Object> dados = new ArrayList<Object>(dadosStr.length);
    for(String numero : dadosStr){
      dados.add(Double.parseDouble(numero));
    }
    collector.emit(dados);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields(keys));
  }
}
