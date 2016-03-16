package ufrj.gta;

import java.util.List;
import java.util.Map;


import org.apache.commons.lang.StringUtils;

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

import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.File;
import java.util.List;
// import java.io.PrintWriter;


public class ResultBolt extends BaseRichBolt {

  private static int contador = 0;
  private static int acertos = 0;
  // private static PrintWriter writer = null;
  private static long minutos = 60*1000;
  private static long tempoInicial;
  private OutputCollector collector;
  transient CountMetric contadorMetrica;
  transient CountMetric acertosMetrica;

  public void initMetrics(TopologyContext context){
    contadorMetrica = new CountMetric();
    acertosMetrica = new CountMetric();
    context.registerMetric("total_processado", contadorMetrica, 60);
    context.registerMetric("total_acertos", acertosMetrica, 60);
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
    collector = outputCollector;
    // try{
    //   writer = new PrintWriter("/vagrant/processados.txt", "UTF-8");
    // }
    // catch(Exception ex3){
    //   System.out.println(ex3);
    // }
    initMetrics(topologyContext);
  }
  @Override
  public void execute(Tuple input) {
    Integer esperado = input.getInteger(0);
    Integer classe = input.getInteger(1);
    // collector.ack(input);
    // if(contador == 0){
    //   tempoInicial = System.currentTimeMillis();
    // }
    contador++;
    contadorMetrica.incr();
    if(esperado == classe){
      acertos++;
      acertosMetrica.incr();
    }
    // long tempo = (System.currentTimeMillis() - tempoInicial);
    // System.out.println(tempo);
    // if(tempo > minutos){
    //   minutos = minutos + 60*1000;
    //   try{
    //     File arquivo = new File("/vagrant/processados.txt");
    //     if(!arquivo.exists()){
    //       arquivo.createNewFile();
    //     }
    //     System.out.println("aqui porra");
    //     FileWriter fileWriter = new FileWriter(arquivo, true);
    //     BufferedWriter bufferWritter = new BufferedWriter(fileWriter);
    //     String mensagem = "Processadas ";
    //     mensagem = mensagem + String.valueOf(contador) + " em " + String.valueOf(minutos - 60*1000) + " ms\n";
    //     bufferWritter.write(mensagem);
    //     bufferWritter.close();
    //   }
    //   catch(Exception ex){
    //     System.out.println(ex);
    //   }
    //   // writer.printf("Processadas %d entradas em %d minutos\n", ResultBolt.getContador(), minutos);
    //   // double acuracia = (double) ResultBolt.getAcertos() / ResultBolt.getContador();
    //   // writer.printf("%.1f%%\n", acuracia * 100.0);
    // }
    // writer.printf("Processadas %d entradas\n", ResultBolt.getContador());
    // double acuracia = (double) ResultBolt.getAcertos() / ResultBolt.getContador();
    // writer.printf("%.1f%%\n", acuracia * 100.0);
    // writer.close();
    // System.out.printf("esperado: %d, obtido: %d\n", esperado, classe);
  }

  public static int getContador(){
    return contador;
  }

  public static int getAcertos(){
    return acertos;
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

  // @Override
  // public void cleanup() {
  //   writer.close();
  // }
}
