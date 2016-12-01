import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import java.net.Socket;


public class TopologiaDRCandidato {

    private TopologiaDRCandidato() {
    }

    private static class PrestaContasSpout extends BaseRichSpout {
        private SpoutOutputCollector _collector;
        private BufferedReader br;
        private String host; // 10.1.1.200;
        private int portNumber; // 9643;
        private Socket socket;
        private PrintWriter out;

        public PrestaContasSpout(String host, int portNumber){
            this.host = host;
            this.portNumber = portNumber;
        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            _collector = collector;


            try {
                socket = new Socket(host, portNumber);
                br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                out = new PrintWriter(socket.getOutputStream(), true);
            } catch (Exception ignored) {
                System.out.println("Falha ao tentar conectar-se ao servidor de emissao");
            }

        }

        @Override
        public void nextTuple(){

            try {
                _collector.emit(new Values(br.readLine()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("tupla"));
        }

    }

    //Bolt to get the entire tuple and filter it with only the name and expenses of the candidate
    private static class FiltraContas extends BaseRichBolt {

        private OutputCollector collector;

        private String sentence;
        private String tipoArquivo;
        private String estadoCandidato;
        private String nomeCandidato;
        private String valorDRS;


        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            // Initialize collector variable with the outputCollector
            collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            // Gets the entire tuple
            // Tuple example: 
            // "D";"07/08/201516:36:03";"250000001515";"SP";"PMDB";"151";"Senador";"ORESTES QUERCIA";"02400871868";"Sim";"Outro";"S/N";"16404287003332";"SUZANO PAPEL E CELULOSE S/A";"02/08/2010";"93036,58";"Baixa de Recursos Estimáveis em Dinheiro";"Outros Recursos nao descritos";"Estimado";"29.713,85 KG DE PAPEL PARA UTILIZAÇÃO EM MATERIAS GRAFICOS"
            sentence = tuple.getString(0);
            // Splits the tuple by ';' character
            String[] splitSentence = sentence.split(";");
            // Gets the type of file Spent or Income
            tipoArquivo = splitSentence[0].replace("\"","");
            // Gets the state of the candidate
            estadoCandidato = splitSentence[3].replace("\"","");
            // Gets the name of the candidate located at the 6th position of the splited array, removing the '"' characters
            nomeCandidato = splitSentence[7].replace("\"", "");
            // Gets the spent value of the tuple, remove the '"', '.' characters and replaces the ',' to '.'
            valorDRS = splitSentence[15].replace("\"", "").replace(".", "").replace(",", ".");
            // Sends the filtered tuple to the designed bolt
            collector.emit(new Values(tipoArquivo, estadoCandidato, nomeCandidato,valorDRS));

        }


        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            // Sets the name of the emmited fields
            outputFieldsDeclarer.declare(new Fields("tipoArquivo", "estadoCandidato", "nomeCandidato","valorDR"));

        }
    }

    // Bolt to sum the candidate expenses
    private static class SomaContasBolt extends BaseRichBolt {
        // Data structure to access and update the candidate sum
        private Map<String, Double> countMap;
        private OutputCollector collector;

        private String nomeCandidato;
        private String valorDRS;
        private String tipoArquivo;
        private String estadoCandidato;
        private double valor;
        private double valorAnterior;

        // Initialize the class methods
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            countMap = new HashMap<String, Double>();
            collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            // Gets from the emmited tuple the candidate name
            nomeCandidato = tuple.getStringByField("nomeCandidato");
            // Gets from the emmited tuple the expense value
            valorDRS = tuple.getStringByField("valorDR");
            // Gets the type of file
            tipoArquivo = tuple.getStringByField("tipoArquivo");
            // Gets the state of the candidate
            estadoCandidato = tuple.getStringByField("estadoCandidato");
            // Converts the String value to double in order to make the sum
            valor = Double.parseDouble(valorDRS);

            // Sums the expense to the candidate amount
            // Creates the entry to the data structure if it is the candidate first entry
            if (countMap.get(nomeCandidato+tipoArquivo) == null) {
                countMap.put(nomeCandidato+tipoArquivo, valor);
            } else { // Sum the candidate expense or income to the total amout saved at the data structure
                valorAnterior = countMap.get(nomeCandidato+tipoArquivo);
                countMap.put(nomeCandidato+tipoArquivo, valor+valorAnterior);
            }

            // Sends the candidate sums to the next bolt
            collector.emit(new Values(tipoArquivo, estadoCandidato, nomeCandidato, countMap.get(nomeCandidato+tipoArquivo).toString()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("tipoArquivo", "estadoCandidato", "nomeCandidato", "valorDR"));
        }
    }

    private static class ImpressorBolt extends BaseRichBolt {
        private String nomeCandidato;
        private String valorDR;
        private String tipoArquivo;
        private String estadoCandidato;
        private String host; // 10.1.1.200;
        private int portNumber; // 9644;
        private Socket socket = null;
        private BufferedReader br;
        private PrintWriter out;

        public ImpressorBolt(String host, int portNumber){
            this.host = host;
            this.portNumber = portNumber;
        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

            try {
                socket = new Socket(host, portNumber);
                br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                out = new PrintWriter(socket.getOutputStream(), true);
            } catch (Exception e) {
                System.out.println("Falha ao tentar conectar-se ao servidor de escrita");
            }
        }

        @Override
        public void execute(Tuple tuple) {

            nomeCandidato = tuple.getStringByField("nomeCandidato");

            valorDR = tuple.getStringByField("valorDR");

            tipoArquivo = tuple.getStringByField("tipoArquivo");

            estadoCandidato = tuple.getStringByField("estadoCandidato");

            out.println(tipoArquivo + "\t" + estadoCandidato + "\t" + nomeCandidato + "\t" + valorDR);

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // nothing to add - since it is the final bolt
        }
    }

    private static class SomaEstadoClassificaBolt extends BaseRichBolt{
        private OutputCollector collector;
        private Map<String, Double> countMap;

        private String nomeCandidato;
        private String valorDRS;
        private String tipoArquivo;
        private String estadoCandidato;
        private double valor;
        private double valorAnterior;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            collector = outputCollector;
            countMap = new HashMap<String,Double>();

        }

        @Override
        public void execute(Tuple tuple) {
            // Gets from the emmited tuple the expense value
            valorDRS = tuple.getStringByField("valorDR");
            // Gets the type of file
            tipoArquivo = tuple.getStringByField("tipoArquivo");
            // Gets the state of the candidate
            estadoCandidato = tuple.getStringByField("estadoCandidato");
            // Converts the String value to double in order to make the sum
            valor = Double.parseDouble(valorDRS);

            // Sums the expense to the candidate amount
            // Creates the entry to the data structure if it is the candidate first entry
            if (countMap.get(estadoCandidato+tipoArquivo) == null) {
                countMap.put(estadoCandidato+tipoArquivo, valor);
            } else { // Sum the candidate expense to the total amount saved at the data structure
                valorAnterior = countMap.get(estadoCandidato+tipoArquivo);
                countMap.put(estadoCandidato+tipoArquivo, valor+valorAnterior);
            }

            // Sends the candidate sums to the next bolt
            collector.emit(new Values(tipoArquivo, estadoCandidato, countMap.get(estadoCandidato+tipoArquivo).toString()));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("tipoArquivo", "estadoCandidato", "totalEstadoDR"));
        }
    }

    private static class ImpressorEstadoBolt extends BaseRichBolt{
        private String nomeCandidato;
        private String valorDR;
        private String tipoArquivo;
        private String estadoCandidato;
        private String host; // 10.1.1.200;
        private int portNumber; // 9644;
        private Socket socket = null;
        private BufferedReader br;
        private PrintWriter out;

        public ImpressorEstadoBolt(String host, int portNumber){
            this.host = host;
            this.portNumber = portNumber;
        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

            try {
                socket = new Socket(host, portNumber);
                br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                out = new PrintWriter(socket.getOutputStream(), true);
            } catch (Exception e) {
                System.out.println("Falha ao tentar conectar-se ao servidor de escrita de Estado");
            }
        }

        @Override
        public void execute(Tuple tuple) {

            valorDR = tuple.getStringByField("totalEstadoDR");

            tipoArquivo = tuple.getStringByField("tipoArquivo");

            estadoCandidato = tuple.getStringByField("estadoCandidato");

            out.println(tipoArquivo + "\t" + estadoCandidato + "\t" +  valorDR);

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // nothing to add - since it is the final bolt
        }
    }


    public static void main(String[] args) throws Exception {
        String ipSocketEmmiter;
        int portaSocketEmmiter;
        String ipSocketReceiver;
        int portaSocketReceiver;
        String ipSocketEstadoReceiver;
        int portaSocketEstadoReceiver;

        TopologyBuilder builder = new TopologyBuilder();
        Config conf = new Config();
        conf.setDebug(true);

        if(args.length == 7) {

            ipSocketEmmiter = args[1];

            try {
                portaSocketEmmiter = Integer.parseInt(args[2]);
            } catch (Exception e){
                portaSocketEmmiter = 9998;
            }

            ipSocketReceiver = args[3];

            try {
                portaSocketReceiver = Integer.parseInt(args[4]);
            } catch (Exception e){
                portaSocketReceiver = 9999;
            }

            ipSocketEstadoReceiver = args[5];

            try {
                portaSocketEstadoReceiver = Integer.parseInt(args[6]);
            } catch (Exception e){
                portaSocketEstadoReceiver = 9999;
            }

            builder.setSpout("prestacao-contas-spout", new PrestaContasSpout(ipSocketEmmiter,portaSocketEmmiter), 1);

            builder.setBolt("filtra-bolt", new FiltraContas(), 20).shuffleGrouping("prestacao-contas-spout");

            builder.setBolt("soma-bolt", new SomaContasBolt(), 20).fieldsGrouping("filtra-bolt", new Fields("nomeCandidato"));

            builder.setBolt("soma-estado-bolt", new SomaEstadoClassificaBolt(), 26).fieldsGrouping("filtra-bolt", new Fields("estadoCandidato"));

            builder.setBolt("impressor-estado-bolt", new ImpressorEstadoBolt(ipSocketEstadoReceiver,portaSocketEstadoReceiver),1).globalGrouping("soma-estado-bolt");

            builder.setBolt("impressor-bolt", new ImpressorBolt(ipSocketReceiver, portaSocketReceiver), 1).globalGrouping("soma-bolt");

            conf.setNumWorkers(15);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

            conf.setMaxTaskParallelism(4);

        } else {
            System.out.println("ERRO: Para a submissao da topologia é necessario 7 argumentos sendo eles:" +
                    "\n1 - Nome da Topologia" +
                    "\n2 - IP do servidor emissor dos dados" +
                    "\n3 - Porta do servidor emissor de dados" +
                    "\n4 - IP do servidor de escrita de dados" +
                    "\n5 - Porta do servidor de escrita de dados" +
                    "\n6 - IP do servidor de escrita de Estado" +
                    "\n7 - Porta do servidor de escrita de Estado");
        }
    }
}
