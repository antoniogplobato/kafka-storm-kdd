package ufrj.gta;

import java.util.List;
import java.util.Map;


import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.List;


public class PrinterBolt extends BaseBasicBolt {

  private String [] keys;

  public PrinterBolt(String [] receivedKeys){
    keys = receivedKeys;
  }

  public void execute(Tuple input, BasicOutputCollector collector) {
    List<Double> dados = (List<Double>) (Object) input.getValues();//tava sem o (Object) antes
    for(Double temp : dados){
      System.out.printf("%f, ", temp);
    }
    System.out.println();
 }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }
}
