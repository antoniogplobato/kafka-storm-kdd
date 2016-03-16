package ufrj.gta;

import java.util.List;
import java.util.Map;


import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;



public class PrinterBolt extends BaseBasicBolt {

  private String [] keys;
  private int qtdKeys;

  public PrinterBolt(String [] receivedKeys){
    keys = receivedKeys;
    qtdKeys = keys.length;
  }

  public void execute(Tuple input, BasicOutputCollector collector) {
    for(String key : keys){
      String temp = input.getStringByField(key);
      if(!StringUtils.isBlank(temp)){
        System.out.println(key + ": " + temp);
      }
    }
 }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }
}
