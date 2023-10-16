package Contador_Palavras;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;


public class Main {
    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("lePalavra", new lePalavras());
        builder.setBolt("contaPalavra", new contaPalavras(),2)
                .shuffleGrouping("lePalavra");


        Config conf = new Config();
        conf.put("arquivoDeLeitura", "/home/coutj/Dados/Estudos/PUC_Minas/10_Processamento_Fluxo_Continuo_Dados/Unidade_3/Contador_Palavras/src/main/java/Contador_Palavras/Files/sample.txt");
        conf.put("diretorioResultado", "/home/coutj/Dados/Estudos/PUC_Minas/10_Processamento_Fluxo_Continuo_Dados/Unidade_3/Contador_Palavras/src/main/java/Contador_Palavras/Files/");
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
