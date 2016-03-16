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
// import org.ejml.ops.CommonOps.elementPower;

public class PredictBolt extends BaseRichBolt
{
  private OutputCollector collector;
  private static DenseMatrix64F theta1T;
  private static DenseMatrix64F theta2T;
  private static boolean executado = false;
  // private String [] keys;
  //
  // public PredictBolt(){
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

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
    collector = outputCollector;
    if(executado == false){
      theta1T = carregarThetaTransposto("/vagrant/kafka-storm-kdd/src/main/java/ufrj/gta/theta1.txt", 50, 42);
      theta2T = carregarThetaTransposto("/vagrant/kafka-storm-kdd/src/main/java/ufrj/gta/theta2.txt", 5, 51);
      executado = true;
    }
  }

  @Override
  public void execute(Tuple tuple)
  {
    // long startTime = System.nanoTime();
    List<Double> dados = (List<Double>) (Object) tuple.getValues();
    double amostra [] = new double[dados.size()];
    for(int i = 0; i < amostra.length; i++){
      amostra[i] = dados.get(i);
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
    // long endTime = System.nanoTime();
    // long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
    // System.out.println(duration);
    collector.emit(new Values(esperado, classe));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
  {
    outputFieldsDeclarer.declare(new Fields("esperado","classe"));
  }
}
