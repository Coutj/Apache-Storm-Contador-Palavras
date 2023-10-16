package Contador_Palavras;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;


public class lePalavras extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private FileReader leitorDeArquivos;
    private BufferedReader reader ;
    private boolean completo = false;

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        this.collector = collector;
        try {
            this.leitorDeArquivos = new FileReader(conf.get("arquivoDeLeitura").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Erro ao ler o arquivo"+conf.get("arquivoDeLeitura")+".");
        }
        this.reader =  new BufferedReader(leitorDeArquivos);
    }


    public void nextTuple()
    {
        if (!completo) {


            try {

                String palavra = reader.readLine();

                if (palavra != null) {
                    palavra = palavra.trim();
                    palavra = palavra.toLowerCase();
                    collector.emit(new Values(palavra));
                }
                else {
                    completo = true;
                    leitorDeArquivos.close();
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Erro ao ler a tupla", e);
            }

        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("palavra"));}

    public static class Main {
        public static void main(String[] args) throws InterruptedException {

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("lePalavra", new lePalavras());
            builder.setBolt("contaPalavra", new contaPalavras(),2)
                    .shuffleGrouping("lePalavra");


            Config conf = new Config();
            conf.put("arquivoDeLeitura", "/home/puc/Documents/sample.txt");
            conf.put("diretorioResultado", "/home/puc/Documents/output/");
            conf.setDebug(true);

            LocalCluster cluster = new LocalCluster();
            try{
                cluster.submitTopology("TopologiaContagemdePalavras", conf, builder.createTopology());
                Thread.sleep(10000);
            }
            finally {
                cluster.shutdown();
            }

        }

    }
}
