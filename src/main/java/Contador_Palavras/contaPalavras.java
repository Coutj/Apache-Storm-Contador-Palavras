package Contador_Palavras;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;


public class contaPalavras extends BaseBasicBolt {

    Map<String, Integer> contagem;
    Integer id;
    String nome;
    String nomeArquivo;

    public void prepare(Map stormConf, TopologyContext context) {
        this.contagem = new HashMap<String, Integer>();
        this.nome = context.getThisComponentId();
        this.id = context.getThisTaskId();
        this.nomeArquivo = stormConf.get("diretorioResultado").toString()+
                "saida"+"-" + nome + id +".txt";
    }


    public void execute(Tuple input,BasicOutputCollector collector) {
        String palavra = input.getString(0);

        if(!contagem.containsKey(palavra)) {
            contagem.put(palavra, 1);
        }else{
            Integer contador = contagem.get(palavra) + 1;
            contagem.put(palavra, contador);
        }

    }


    public void cleanup() {

        try{
            PrintWriter writer = new PrintWriter(nomeArquivo, "UTF-8");

            for(Map.Entry<String, Integer> entry : contagem.entrySet()){
                writer.println(entry.getKey()+": "+entry.getValue());
            }

            writer.close();
        }
        catch (Exception e){}


    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
