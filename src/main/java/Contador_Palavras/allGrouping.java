package Contador_Palavras;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class allGrouping {
    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("lePalavra", new lePalavras());
        builder.setBolt("contaPalavra", new contaPalavras(),2)
                .allGrouping("lePalavra");


        Config conf = new Config();
        conf.put("arquivoDeLeitura", "/home/puc/Documents/sample.txt");
        conf.put("diretorioResultado", "/home/puc/Documents/output/");
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("TopologiaContagemdePalavras", conf, builder.createTopology());
            Thread.sleep(50000);
        }
        finally {
            cluster.shutdown();
        }

    }

}