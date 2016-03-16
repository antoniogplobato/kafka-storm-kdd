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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import java.lang.Math;

import org.ejml.data.DenseMatrix64F;
import org.ejml.ops.CommonOps;

import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;

public class AllBolt extends BaseRichBolt
{
  private OutputCollector collector;
  private String [] keys;

  private static DenseMatrix64F theta1T;
  private static DenseMatrix64F theta2T;
  private static boolean executado = false;

  private static int contador = 0;
  private static int acertos = 0;
  transient CountMetric contadorMetrica;
  transient CountMetric acertosMetrica;

  // public AllBolt(String [] receivedKeys){
  //   keys = receivedKeys;
  // }

  public static DenseMatrix64F carregarThetaTransposto(String arquivoStr, int nrLinhas, int nrColunas){
    DenseMatrix64F matrizTransposta = new DenseMatrix64F(nrColunas, nrLinhas);
    Scanner scanner;
    try{
      scanner = new Scanner(new File(arquivoStr));
    }
    catch(FileNotFoundException e){
      System.out.println(e);
      System.exit(0);
      return matrizTransposta;
    }
    scanner.useDelimiter(" |\\n");
    int contadorColuna = 0;
    int contadorLinha = 0;
    while(scanner.hasNext() != false){
      if(contadorColuna == nrColunas){
        contadorColuna = 0;
        contadorLinha++;
      }
      matrizTransposta.set(contadorColuna, contadorLinha, Double.parseDouble(scanner.next()));
      contadorColuna++;
    }
    scanner.close();
    return matrizTransposta;
  }

  public static void sigmoid(DenseMatrix64F matriz){
    CommonOps.changeSign(matriz);
    CommonOps.elementPower(Math.E, matriz, matriz);
    CommonOps.add(matriz, 1.0);
    CommonOps.divide(1.0, matriz);
  }

  public void initMetrics(TopologyContext context){
    contadorMetrica = new CountMetric();
    acertosMetrica = new CountMetric();
    context.registerMetric("total_processado", contadorMetrica, 60);
    context.registerMetric("total_acertos", acertosMetrica, 60);
  }

  public static int getContador(){
    return contador;
  }

  public static int getAcertos(){
    return acertos;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
    collector = outputCollector;
    initMetrics(topologyContext);
    if(executado == false){
      theta1T = carregarThetaTransposto("/vagrant/kafka-storm-kdd/src/main/java/ufrj/gta/theta1.txt", 50, 42);
      theta2T = carregarThetaTransposto("/vagrant/kafka-storm-kdd/src/main/java/ufrj/gta/theta2.txt", 5, 51);
      executado = true;
    }
  }

  @Override
  public void execute(Tuple tuple)
  {
    String[] dadosStr = tuple.getString(0).split(" ");
    double amostra [] = new double[dadosStr.length];
    int indice = 0;
    for(String numero : dadosStr){
      amostra[indice] = Double.parseDouble(numero);
      indice++;
    }
    DenseMatrix64F X = new DenseMatrix64F(1,42,true,amostra);
    int esperado = (int) X.get(0,41);
    CommonOps.extract(X,0,1,0,41,X,0,1);
    X.set(0,0,1.0);
    // CommonOps.transpose(theta1);
    DenseMatrix64F h1 = new DenseMatrix64F(X.numRows,theta1T.numCols);
    CommonOps.mult(X, theta1T, h1);
    sigmoid(h1);
    DenseMatrix64F h1exp = new DenseMatrix64F(1,51);
    CommonOps.extract(h1,0,1,0,50,h1exp,0,1);
    h1exp.set(0,0,1.0);
    // CommonOps.transpose(theta2);
    DenseMatrix64F h2 = new DenseMatrix64F(h1exp.numRows,theta2T.numCols);
    CommonOps.mult(h1exp, theta2T, h2);
    sigmoid(h2);
    Double maximo = -1.1;
    int classe = 0;
    for(int i = 0; i < h2.data.length; i++){
      if(h2.data[i] > maximo){
        maximo = h2.data[i];
        classe = i + 1;
      }
    }
    contador++;
    contadorMetrica.incr();
    if(esperado == classe){
      acertos++;
      acertosMetrica.incr();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){

  }
}
